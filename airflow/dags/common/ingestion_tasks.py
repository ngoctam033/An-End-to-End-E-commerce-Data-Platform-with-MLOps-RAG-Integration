import logging

import pandas as pd
import io
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task, get_current_context
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.compute as pc
from utils.path_node import PathManager

# Cấu hình logging
logger = logging.getLogger("airflow.task")

@task
def ingest_task():
    """
    Logic lấy context và thực thi phải nằm bên trong một @task
    """
    # Lấy context khi task đang thực thi
    context = get_current_context()
    ds = context['ds'] # Lấy chuỗi YYYY-MM-DD (ví dụ: 2026-01-19)
    return ds
@task
def extract_and_load_to_minio(table_name: str,
                              bucket_name: str,
                              extraction_date: datetime,
                              folder: PathManager,
                              file_name: str,
                              date_column: str = None):
    """
    Quy trình: Extract (Postgres) -> Transform (Pandas to Parquet) -> Load (MinIO)
    Có xử lý ngoại lệ và raise lỗi để Airflow task failure.
    """
    
    # 1. Kết nối Postgres và Extract dữ liệu
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        # Sử dụng parameter binding để an toàn hơn thay vì f-string trực tiếp cho value
        if date_column:
            sql = f"SELECT * FROM {table_name} WHERE DATE({date_column}) = %s;"
            logger.info(f"Trích xuất dữ liệu từ bảng {table_name} cho ngày {extraction_date} dùng cột {date_column}...")
            df = pg_hook.get_pandas_df(sql, parameters=(extraction_date,))
        else:
            sql = f"SELECT * FROM {table_name};"
            logger.info(f"Trích xuất TOÀN BỘ dữ liệu từ bảng {table_name}...")
            df = pg_hook.get_pandas_df(sql)

        if df.empty:
            logger.warning(f"Bảng {table_name} không có dữ liệu cho ngày {extraction_date}.")
            return

    except Exception as e:
        logger.error(f"Lỗi xảy ra trong quá trình Extract từ Postgres: {str(e)}")
        raise

    # 2. Transform: Chuyển đổi sang Parquet
    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer,
                        index=False,
                        engine='pyarrow',
                        coerce_timestamps='us',       # Ép về Microseconds (Spark hiểu được)
                        allow_truncated_timestamps=True) # Cho phép cắt bỏ phần đuôi nano thừa
        parquet_buffer.seek(0)
    except Exception as e:
        logger.error(f"Lỗi xảy ra khi biến đổi dữ liệu sang Parquet: {str(e)}")
        raise

    # 3. Load: Upload lên MinIO bằng S3Hook
    try:
        object_path = f"{folder}/{file_name}"
        s3_hook = S3Hook(aws_conn_id='minio_default')

        # Kiểm tra và tạo bucket nếu cần
        if not s3_hook.check_for_bucket(bucket_name):
            logger.warning(f"Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            s3_hook.create_bucket(bucket_name)

        s3_hook.load_file_obj(
            file_obj=parquet_buffer,
            key=object_path,
            bucket_name=bucket_name,
            replace=True
        )
        
    except Exception as e:
        logger.error(f"Lỗi xảy ra trong quá trình Load lên MinIO: {str(e)}")
        raise

@task
def transform_with_pyiceberg(table_name: str, source_table: str, target_table: str, ds: str = None):
    """
    Transform dữ liệu sử dụng PyIceberg + PyArrow thay vì Spark (nhẹ hơn cho máy cấu hình yếu)
    Đọc từ Iceberg Raw -> Transform -> Ghi vào Iceberg Silver
    """
    
    try:
        logger.info(f"[{table_name}] Khởi tạo PyIceberg với cấu hình S3/MinIO")
        
        # Cấu hình S3 filesystem cho PyIceberg
        from pyiceberg.io.pyarrow import PyArrowFileIO
        
        # Tạo FileIO với cấu hình S3/MinIO
        file_io_props = {
            "s3.endpoint": "http://minio1:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "admin123",
            "s3.path-style-access": "true"
        }
        
        logger.info(f"[{table_name}] 1. Extract: Đọc từ {source_table}")
        
        # Parse namespace và table name từ source_table (e.g., "iceberg.raw.geo_location")
        namespace_parts = source_table.split(".")
        # Với format "iceberg.raw.geo_location":
        # - catalog = "iceberg"
        # - namespace = "raw"
        # - table = "geo_location"
        if len(namespace_parts) >= 3:
            catalog_name = namespace_parts[0]  # "iceberg"
            namespace = namespace_parts[1]      # "raw"
            table_only_name = namespace_parts[2]  # "geo_location"
        else:
            raise ValueError(f"Invalid source_table format: {source_table}. Expected format: catalog.namespace.table")
        
        # Xây dựng đường dẫn đến metadata trên S3
        # Theo cấu trúc Spark Iceberg sử dụng: s3://datalake/<namespace>/<table>/metadata/
        metadata_location = f"s3://datalake/{namespace}/{table_only_name}/metadata"
        
        logger.info(f"[{table_name}] Metadata location: {metadata_location}")
        
        # Load table trực tiếp từ metadata location
        from pyiceberg.table import StaticTable
        
        # Tìm version metadata mới nhất
        import s3fs
        s3 = s3fs.S3FileSystem(
            key="admin",
            secret="admin123",
            client_kwargs={"endpoint_url": "http://minio1:9000"},
            use_ssl=False
        )
        
        # List metadata files và tìm file version mới nhất
        metadata_files = s3.glob(f"{metadata_location}/*.metadata.json")
        if not metadata_files:
            raise FileNotFoundError(f"No metadata files found at {metadata_location}")
        
        # Lấy file metadata mới nhất (sort theo tên file)
        latest_metadata = sorted(metadata_files)[-1]
        latest_metadata_path = f"s3://{latest_metadata}"
        
        # Load static table
        source_tbl = StaticTable.from_metadata(latest_metadata_path, properties=file_io_props)
        
        # Khởi tạo Catalog để sử dụng cho Target Table
        catalog = load_catalog(
            catalog_name,
            **{
                **file_io_props,
                "type": "rest",
                "uri": "http://minio1:9000",
                "warehouse": "s3://datalake"
            }
        )
        
        # Scan và đọc dữ liệu vào PyArrow Table
        scan = source_tbl.scan()
        arrow_table = scan.to_arrow()
        
        # Filter theo ngày sử dụng PyArrow
        if ds and 'ingestion_date' in arrow_table.column_names:
            logger.info(f"[{table_name}] Filtering data for date: {ds}")
            col = arrow_table['ingestion_date']
            # Kiểm tra kiểu dữ liệu của ingestion_date để so khớp phù hợp
            if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
                # Nếu là string (ví dụ '2026-02-04'), so sánh trực tiếp với ds
                filter_mask = pc.equal(col, ds)
            elif pa.types.is_timestamp(col.type):
                # Nếu là timestamp, dùng strftime để format về string YYYY-MM-DD
                filter_mask = pc.equal(pc.strftime(col, '%Y-%m-%d'), ds)
            else:
                # Trường hợp khác (ví dụ Date32), cast về string rồi so sánh
                filter_mask = pc.equal(col.cast(pa.string()), ds)
                
            arrow_table = arrow_table.filter(filter_mask)
        
        if arrow_table.num_rows == 0:
            logger.warning(f"[{table_name}] Không có dữ liệu cho ngày {ds}")
            return
            
        logger.info(f"[{table_name}] Đã đọc {arrow_table.num_rows} dòng dữ liệu")
        
        # 2. Base Transform: Thêm metadata
        logger.info(f"[{table_name}] 2. Base Transform: Thêm Metadata")
        
        # Thêm cột processed_at
        processed_at_col = pa.array([pd.Timestamp.now()] * arrow_table.num_rows, type=pa.timestamp('us'))
        arrow_table = arrow_table.append_column('processed_at', processed_at_col)
        
        # Thêm cột _source_table
        source_table_col = pa.array([source_table] * arrow_table.num_rows, type=pa.string())
        arrow_table = arrow_table.append_column('_source_table', source_table_col)
        
        # 3. Specific Transform cho geo_location (nếu cần)
        logger.info(f"[{table_name}] 3. Transform: Xử lý đặc thù")
        # Thêm logic transform đặc thù cho geo_location ở đây nếu cần
        
        # 4. Load: Ghi vào Silver Zone
        logger.info(f"[{table_name}] 4. Load: Ghi dữ liệu vào {target_table}")
        
        # Parse target table
        target_namespace_parts = target_table.split(".")
        if len(target_namespace_parts) >= 3:
            # Ví dụ: iceberg.silver.geo_location -> namespace là silver, table là geo_location
            target_namespace = target_namespace_parts[1]
            target_table_name = target_namespace_parts[2]
        else:
            target_namespace = ".".join(target_namespace_parts[:-1])
            target_table_name = target_namespace_parts[-1]
        
        # Kiểm tra và tạo target table nếu chưa tồn tại
        try:
            target_tbl = catalog.load_table((target_namespace, target_table_name))
            logger.info(f"[{table_name}] Table {target_table} đã tồn tại, sẽ append dữ liệu")
            
            # Append dữ liệu
            target_tbl.append(arrow_table)
            
        except Exception:
            logger.info(f"[{table_name}] Tạo table mới {target_table}")
            
            # Tạo namespace nếu chưa có
            try:
                catalog.create_namespace(target_namespace)
            except Exception:
                pass  # Namespace đã tồn tại
            
            # Tạo table mới với schema từ arrow_table
            # Convert PyArrow schema sang Iceberg schema một cách chuẩn xác
            from pyiceberg.io.pyarrow import pyarrow_to_schema
            schema = pyarrow_to_schema(arrow_table.schema)
            
            # Tạo table
            target_tbl = catalog.create_table(
                identifier=(target_namespace, target_table_name),
                schema=schema,
                partition_spec=None  # Có thể thêm partition spec nếu cần
            )
            
            # Ghi dữ liệu
            target_tbl.append(arrow_table)
        
        logger.info(f"[{table_name}] Hoàn thành transformation với PyIceberg")
        
    except Exception as e:
        logger.error(f"[{table_name}] Lỗi trong quá trình transform với PyIceberg: {str(e)}")
        raise
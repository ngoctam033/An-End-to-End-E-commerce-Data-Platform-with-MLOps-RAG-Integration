import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, col, trim, upper, current_timestamp, days, lit
from pyspark.sql.types import DecimalType
from abc import ABC, abstractmethod

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iceberg_transformation")

class BaseIcebergTransformer(ABC):
    def __init__(self, table_name, ds, source_table, target_table):
        self.table_name = table_name
        self.ds = ds
        self.source_table = source_table
        self.target_table = target_table
        
        spark_conf = SparkConf()
        
        # Cấu hình JVM tối ưu cho Java 17 và máy yếu
        jvm_flags = (
            "-XX:+UseG1GC "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "-Dio.netty.tryReflectionSetAccessible=true"
        )

        defaults = {
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'hadoop',
            'spark.sql.catalog.iceberg.warehouse': 's3a://datalake',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio1:9000',
            'spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'admin123',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.metrics.enabled': 'false',
            # CẤU HÌNH TÀI NGUYÊN (Tối ưu cho worker 2GB RAM)
            'spark.cores.max': '2',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '512m',
            'spark.executor.memoryOverhead': '256m',
            # GIẢM TỐI ĐA SỰ SONG SONG ĐỂ TRÁNH LỖI 134 VÀ SHUFFLE FAILURE
            'spark.sql.shuffle.partitions': '1',
            'spark.default.parallelism': '1',
            'spark.memory.fraction': '0.7',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.iceberg.handle-timestamp-without-timezone': 'true',
            'spark.executor.extraJavaOptions': jvm_flags,
            'spark.driver.extraJavaOptions': jvm_flags
        }
        
        for k, v in defaults.items():
            if not spark_conf.contains(k):
                spark_conf.set(k, v)

        self.spark = SparkSession.builder \
            .appName(f"Transform_{table_name}_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()
        self.df = None

    def extract(self):
        logger.info(f"[{self.table_name}] 1. Extraction: Đọc từ {self.source_table}")
        self.df = self.spark.table(self.source_table) \
            .filter(col("ingestion_date") == self.ds)
        return self

    def base_transform(self):
        if self.df is not None:
            self.df = self.df \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("_source_table", lit(self.source_table))
        return self

    def _create_table(self):
        logger.info(f"[{self.table_name}] Khởi tạo bảng target mới: {self.target_table}")
        self.df.coalesce(1).writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("created_at")) \
            .create()

    @abstractmethod
    def transform(self):
        return self

    def prepare_target_table(self):
        """Kiểm tra namespace và tạo bảng target nếu chưa tồn tại trước khi extract."""
        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[{self.table_name}] 0. Prepare: Bảng {self.target_table} chưa tồn tại. Đang tạo...")
            # Lấy schema bằng cách chạy flow transform trên dữ liệu rỗng
            temp_df = self.spark.table(self.source_table).limit(0)
            original_df = self.df
            self.df = temp_df
            self.base_transform().transform()
            self._create_table()
            # Reset lại df để không ảnh hưởng đến bước extract
            self.df = original_df
        else:
            logger.info(f"[{self.table_name}] 0. Prepare: Bảng {self.target_table} đã tồn tại.")
        
        return self

    def run(self):
        """Thực thi toàn bộ pipeline: Prepare -> Extract -> Transform -> Load"""
        try:
            self.prepare_target_table() \
                .extract() \
                .base_transform() \
                .transform() \
                .load()
        except Exception as e:
            logger.error(f"[{self.table_name}] Lỗi khi chạy pipeline: {str(e)}")
            raise
        finally:
            if self.spark:
                logger.info(f"[{self.table_name}] Đóng Spark Session.")
                self.spark.stop()

    def load(self):
        if self.df is None:
            return self

        logger.info(f"[{self.table_name}] 5. Load: Bắt đầu quá trình ghi dữ liệu vào {self.target_table}")

        # Bảng đã được chuẩn bị ở bước prepare_target_table, chỉ thực hiện append
        # Sắp xếp nhẹ trước khi ghi để giảm áp lực ghi vào Partition của Iceberg
        self.df.writeTo(self.target_table).append()
        logger.info(f"[{self.table_name}] 5. Load: Hoàn tất ghi dữ liệu.")
        return self

class GeoLocationTransformer(BaseIcebergTransformer):
    """Transformer riêng cho geo_location tối ưu theo partition province_name"""

    def load(self):
        if self.df is None:
            return self

        logger.info(f"[{self.table_name}] 5. Load: Ghi dữ liệu geo_location tối ưu (repartition by province_name)")
        
        # Bảng đã được chuẩn bị/tạo ở bước prepare_target_table (run pipeline)
        self.df.repartition(col("province_name")).writeTo(self.target_table).append()
            
        logger.info(f"[{self.table_name}] 5. Load: Hoàn tất ghi dữ liệu cho GeoLocation.")
        return self
        
    def transform(self):
        return self

    def _create_table(self):
        logger.info(f"[{self.table_name}] Khởi tạo bảng target mới với partition province_name")
        self.df.repartition(col("province_name")).writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("province_name") \
            .create()

class OrdersTransformer(BaseIcebergTransformer):
    def transform(self):
        if self.df is not None:
            logger.info(f"[{self.table_name}] 4. Specific Transform: Orders")
            self.df = self.df \
                .withColumn("order_date", from_utc_timestamp(col("order_date"), "GMT+7")) \
                .withColumn("created_at", from_utc_timestamp(col("created_at"), "GMT+7")) \
                .withColumn("total_price", col("total_price").cast(DecimalType(18, 2))) \
                .withColumn("status", upper(trim(col("status").cast("string")))) \
                .filter(col("total_price") >= 0)
        return self

class OrderItemsTransformer(BaseIcebergTransformer):
    def transform(self):
        if self.df is not None:
            logger.info(f"[{self.table_name}] 4. Specific Transform: Order Items")
            self.df = self.df \
                .withColumn("unit_price", col("unit_price").cast(DecimalType(18, 2))) \
                .withColumn("amount", col("amount").cast(DecimalType(18, 2))) \
                .dropna(subset=["order_id", "product_id"]) \
                .dropDuplicates(["order_id", "product_id"])
        return self

class DefaultTransformer(BaseIcebergTransformer):
    def transform(self):
        return self

def main():
    if len(sys.argv) < 5:
        sys.exit(1)

    ds, table_name, source_table, target_table = sys.argv[1:5]
    registry = {
        "orders": OrdersTransformer, 
        "order_items": OrderItemsTransformer,
        "geo_location": GeoLocationTransformer
    }
    transformer_cls = registry.get(table_name, DefaultTransformer)
    
    # Khởi tạo và chạy pipeline
    transformer = transformer_cls(table_name, ds, source_table, target_table)
    transformer.run()

if __name__ == "__main__":
    main()
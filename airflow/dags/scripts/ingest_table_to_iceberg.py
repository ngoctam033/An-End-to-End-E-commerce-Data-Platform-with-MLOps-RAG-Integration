import sys
import logging
from abc import ABC
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import math

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest_to_iceberg")

class BaseIcebergIngestor(ABC):
    """
    Lớp cơ sở tập trung vào vai trò Extract & Load (EL).
    Lấy dữ liệu từ Source (Postgres) và lưu trữ nguyên bản vào Raw Zone (Iceberg).
    """
    def __init__(self, table_name, ds, sql_query, target_table, primary_key=None):
        self.table_name = table_name
        self.ds = ds
        self.sql_query = sql_query
        self.primary_key = primary_key
        self.target_table = target_table
        spark_conf = SparkConf()
        
        # Cấu hình mặc định
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
            # CẤU HÌNH TÀI NGUYÊN (Tối ưu cho 12 Cores / 8GB RAM Total)
            # Worker: 7GB -> Chạy 2 Executor (2GB + 512MB Overhead) + Driver (1GB) = 6GB Total Used
            'spark.cores.max': '2',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '512m',
            'spark.executor.memoryOverhead': '256m',
        }
        
        for k, v in defaults.items():
            if not spark_conf.contains(k):
                spark_conf.set(k, v)

        self.spark = SparkSession.builder \
            .appName(f"Ingest_{table_name}_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()
        self.df = None
        logger.info(f"[{self.table_name}] Khởi tạo Ingestor cho bảng {table_name} với ds={ds}")
        logger.info(f"[{self.table_name}] SQL Query: {sql_query}")
        logger.info(f"[{self.table_name}] Target Table: {target_table}")
        logger.info(f"[{self.table_name}] Primary Key: {primary_key}")
        # Cấu hình Postgres JDBC
        self.jdbc_url = "jdbc:postgresql://db:5432/ecommerce_db"
        self.connection_properties = {
            "user": "postgres",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }

    def extract(self):
        """Bước 1: Extract - Lấy dữ liệu từ source SQL query"""
        logger.info(f"[{self.table_name}] 1. Extract: Đang truy vấn dữ liệu từ Postgres")
        query = f"({self.sql_query}) as source"
        
        self.df = self.spark.read.jdbc(
            url=self.jdbc_url, 
            table=query, 
            properties=self.connection_properties
        )
        
        # Thêm metadata tối thiểu để quản lý Raw Zone (audit columns)
        if self.df is not None and self.df.count() > 0:
            self.df = self.df \
                .withColumn("ingested_at", current_timestamp()) \
                .withColumn("ingestion_date", lit(self.ds))
        else:
            logger.warning(f"[{self.table_name}] Extract: Không có dữ liệu để xử lý.")
            self.df = None
            
        return self

    def load(self):
        """Bước 2: Load - Lưu dữ liệu vào Raw Zone (Iceberg)"""
        if self.df is None:
            return self
        # in ra full path của s3
        

        logger.info(f"[{self.table_name}] 2. Load: Đang lưu dữ liệu vào {self.target_table}")
        
        # Đảm bảo Namespace tồn tại
        namespace = ".".join(self.target_table.split(".")[:-1])
        logger.info(f"[{self.table_name}] Namespace: {namespace}")
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        # Ghi dữ liệu
        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[{self.table_name}] Khởi tạo bảng mới")
            self.df.writeTo(self.target_table) \
                .tableProperty("format-version", "2") \
                .partitionedBy("created_at") \
                .create()
        else:
            logger.info(f"[{self.table_name}] Append dữ liệu vào bảng hiện tại")
            self.df.writeTo(self.target_table).append()
            
        logger.info(f"[{self.table_name}] Hoàn thành Extract & Load.")
        return self

class GeoLocationIngestor(BaseIcebergIngestor):
    """Xử lý riêng cho bảng geo_location với lô 5000 dòng"""
    def load(self):
        if self.df is None:
            return self

        logger.info(f"[{self.table_name}] 2. Load (Batch): Đang lưu geo_location theo lô 5000 dòng")
        
        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        total_rows = self.df.count()
        num_partitions = math.ceil(total_rows / 10000)
        logger.info(f"[{self.table_name}] Tổng số dòng: {total_rows}. Chia thành {num_partitions} files.")

        # Thêm table property để Iceberg biết chia file nhỏ hơn (nếu cần)
        # Nhưng quan trọng nhất là repartition + partitionBy
        writer = self.df.repartition(num_partitions).writeTo(self.target_table)
        
        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[{self.table_name}] Khởi tạo bảng mới với property batching")
            writer.tableProperty("format-version", "2") \
                .tableProperty("write.target-file-size-bytes", "536870912") \
                .partitionedBy("ingestion_date") \
                .create()
        else:
            logger.info(f"[{self.table_name}] Append dữ liệu vào bảng hiện tại")
            writer.append()
            
        logger.info(f"[{self.table_name}] Hoàn thành Extract & Load cho GeoLocation.")
        return self

class DefaultIngestor(BaseIcebergIngestor):
    """Sử dụng trực tiếp logic mặc định của base class cho mục tiêu EL"""
    pass

def main():
    if len(sys.argv) < 5:
        print("Usage: ingest_table_to_iceberg.py <table_name> <ds> <sql_query> <target_table> [primary_key]")
        sys.exit(1)
    
    table_name = sys.argv[1]
    ds = sys.argv[2]
    sql_query = sys.argv[3]
    target_table = sys.argv[4]
    primary_key = sys.argv[5] if len(sys.argv) > 5 else None

    registry = {"geo_location": GeoLocationIngestor}
    ingestor_cls = registry.get(table_name, DefaultIngestor)
    
    ingestor = ingestor_cls(table_name, ds, sql_query, target_table, primary_key)
    ingestor.extract().load()

if __name__ == "__main__":
    main()

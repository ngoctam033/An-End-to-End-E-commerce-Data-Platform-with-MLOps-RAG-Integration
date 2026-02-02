import sys
import logging
from abc import ABC
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest_to_iceberg")

class BaseIcebergIngestor(ABC):
    """
    Lớp cơ sở tập trung vào vai trò Extract & Load (EL).
    Lấy dữ liệu từ Source (Postgres) và lưu trữ nguyên bản vào Raw Zone (Iceberg).
    """
    def __init__(self, table_name, ds, sql_query):
        self.table_name = table_name
        self.ds = ds
        self.sql_query = sql_query
        self.spark = SparkSession.builder \
            .appName(f"Ingest_{table_name}_{ds}") \
            .getOrCreate()
        self.df = None
        self.target_table = f"iceberg.raw.{table_name}"
        
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

        logger.info(f"[{self.table_name}] 2. Load: Đang lưu dữ liệu raw vào {self.target_table}")
        
        # Đảm bảo Namespace tồn tại
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.raw")

        # Ghi dữ liệu (Append mode cho Raw Zone)
        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[{self.table_name}] Khởi tạo bảng Raw mới")
            self.df.writeTo(self.target_table) \
                .tableProperty("format-version", "2") \
                .create()
        else:
            logger.info(f"[{self.table_name}] Append dữ liệu vào bảng Raw hiện tại")
            self.df.writeTo(self.target_table).append()
            
        logger.info(f"[{self.table_name}] Hoàn thành Extract & Load.")
        return self

class DefaultIngestor(BaseIcebergIngestor):
    """Sử dụng trực tiếp logic mặc định của base class cho mục tiêu EL"""
    pass

def main():
    if len(sys.argv) < 4:
        print("Usage: ingest_table_to_iceberg.py <table_name> <ds> <sql_query>")
        sys.exit(1)
    
    table_name = sys.argv[1]
    ds = sys.argv[2]
    sql_query = sys.argv[3]

    # Khởi tạo Ingestor
    ingestor = DefaultIngestor(table_name, ds, sql_query)
    
    # Thực hiện chuỗi Extract & Load
    ingestor.extract().load()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Lỗi trong quá trình Ingest (EL): {str(e)}")
        sys.exit(1)

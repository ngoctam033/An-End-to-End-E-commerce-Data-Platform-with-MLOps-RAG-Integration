import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, col, trim, upper, current_timestamp, days
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import lit
from abc import ABC, abstractmethod

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iceberg_transformation")

class BaseIcebergTransformer(ABC):
    def __init__(self, table_name, ds):
        self.table_name = table_name
        self.ds = ds
        self.spark = SparkSession.builder \
            .appName(f"Transform_{table_name}_{ds}") \
            .getOrCreate()
        self.df = None
        self.raw_table = f"iceberg.raw.{self.table_name}"
        self.silver_table = f"iceberg.silver.{self.table_name}"
        self.primary_key = None # Sẽ được định nghĩa ở lớp con

    def extract(self):
        logger.info(f"[{self.table_name}] 1. Extraction: Đọc từ {self.raw_table}")
        self.df = self.spark.table(self.raw_table).filter(col("ingestion_date") == self.ds)
        return self

    def base_transform(self):
        """Thêm các cột Audit Metadata cho Data Lake"""
        if self.df is not None:
            logger.info(f"[{self.table_name}] 3. Base Transform: Thêm Metadata")
            self.df = self.df \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("_source_table", lit(self.raw_table))
        return self

    @abstractmethod
    def transform(self):
        pass

    def load(self):
        """Lưu dữ liệu vào bảng Silver bằng DataFrame API"""
        if self.df is None or self.df.count() == 0:
            logger.warning(f"[{self.table_name}] 5. Load: Không có dữ liệu để ghi.")
            return self

        # Kiểm tra và tạo bảng nếu chưa tồn tại
        if not self.spark.catalog.tableExists(self.silver_table):
            # Tự động tạo Namespace nếu cần (Iceberg Spark 3 sẽ tự xử lý nếu cấu hình đúng)
            self._create_table()
        else:
            logger.info(f"[{self.table_name}] 5. Load: Ghi đè Partitions vào {self.silver_table}")
            # Sử dụng overwritePartitions để đảm bảo tính Idempotency cho ELT
            self.df.writeTo(self.silver_table).overwritePartitions()
        
        return self

    def _create_table(self):
        logger.info(f"[{self.table_name}] Khởi tạo bảng Silver mới")
        self.df.writeTo(self.silver_table).tableProperty("format-version", "2").create()

class OrdersTransformer(BaseIcebergTransformer):
    def __init__(self, table_name, ds):
        super().__init__(table_name, ds)
        self.primary_key = "id"

    def transform(self):
        if self.df is not None:
            logger.info(f"[{self.table_name}] 4. Specific Transform: Chuẩn hóa dữ liệu Orders")
            self.df = self.df \
                .withColumn("order_date", from_utc_timestamp(col("order_date"), "GMT+7")) \
                .withColumn("created_at", from_utc_timestamp(col("created_at"), "GMT+7")) \
                .withColumn("total_price", col("total_price").cast(DecimalType(18, 2))) \
                .withColumn("status", upper(trim(col("status").cast("string")))) \
                .filter(col("total_price") >= 0) \
                .sortWithinPartitions("id") # Tối ưu cấu trúc file
        return self

    def _create_table(self):
        self.df.writeTo(self.silver_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("order_date")) \
            .create()

class OrderItemsTransformer(BaseIcebergTransformer):
    def __init__(self, table_name, ds):
        super().__init__(table_name, ds)
        self.primary_key = "id"

    def transform(self):
        if self.df is not None:
            logger.info(f"[{self.table_name}] 4. Specific Transform: Chuẩn hóa dữ liệu Order Items")
            self.df = self.df \
                .withColumn("unit_price", col("unit_price").cast(DecimalType(18, 2))) \
                .withColumn("total_price", col("total_price").cast(DecimalType(18, 2)))
        return self

    def _create_table(self):
        self.df.writeTo(self.silver_table) \
            .tableProperty("format-version", "2") \
            .create()

def main():
    if len(sys.argv) < 2:
        print("Usage: transform_table.py <ds> [table_name]")
        sys.exit(1)

    ds = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else "orders"

    registry = {
        "orders": OrdersTransformer,
        "order_items": OrderItemsTransformer
    }

    transformer_cls = registry.get(table_name)
    if not transformer_cls:
        logger.error(f"Không tìm thấy Transformer cho bảng: {table_name}")
        sys.exit(1)

    transformer = transformer_cls(table_name, ds)
    transformer.extract() \
               .base_transform() \
               .transform() \
               .load()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Lỗi Fatal: {str(e)}")
        sys.exit(1)
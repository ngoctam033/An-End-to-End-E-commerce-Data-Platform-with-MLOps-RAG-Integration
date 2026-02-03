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
    def __init__(self, table_name, ds, source_table, target_table):
        self.table_name = table_name
        self.ds = ds
        self.source_table = source_table
        self.target_table = target_table
        self.spark = SparkSession.builder \
            .appName(f"Transform_{table_name}_{ds}") \
            .getOrCreate()
        self.df = None
        self.primary_key = None # Sẽ được định nghĩa ở lớp con

    def extract(self):
        logger.info(f"[{self.table_name}] 1. Extraction: Đọc từ {self.source_table}")
        self.df = self.spark.table(self.source_table).filter(col("ingestion_date") == self.ds)
        return self

    def base_transform(self):
        """Thêm các cột Audit Metadata cho Data Lake"""
        if self.df is not None:
            logger.info(f"[{self.table_name}] 3. Base Transform: Thêm Metadata")
            self.df = self.df \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("_source_table", lit(self.source_table))
        return self

    @abstractmethod
    def transform(self):
        pass

    def load(self):
        """Sử dụng MERGE INTO để bảo vệ tính nhất quán của dữ liệu (Idempotency)"""
        if self.df is None or self.df.count() == 0:
            logger.warning(f"[{self.table_name}] 5. Load: Không có dữ liệu để ghi.")
            return self

        # Đảm bảo Namespace tồn tại
        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        if not self.spark.catalog.tableExists(self.target_table):
            self._create_table()
        else:
            if self.primary_key:
                logger.info(f"[{self.table_name}] 5. Load: Thực hiện MERGE INTO tại {self.target_table} dựa trên {self.primary_key}")
                self.df.createOrReplaceTempView("source_view")
                merge_sql = f"""
                    MERGE INTO {self.target_table} t
                    USING source_view s
                    ON t.{self.primary_key} = s.{self.primary_key}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
            else:
                logger.info(f"[{self.table_name}] 5. Load: Ghi đè Partitions (Dùng cho bảng không có PK)")
                self.df.writeTo(self.target_table).overwritePartitions()
        
        return self

    def _create_table(self):
        logger.info(f"[{self.table_name}] Khởi tạo bảng target mới: {self.target_table}")
        self.df.writeTo(self.target_table).tableProperty("format-version", "2").create()

class OrdersTransformer(BaseIcebergTransformer):
    def __init__(self, table_name, ds, source_table, target_table):
        super().__init__(table_name, ds, source_table, target_table)
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
        self.df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("order_date")) \
            .create()

class OrderItemsTransformer(BaseIcebergTransformer):
    def __init__(self, table_name, ds, source_table, target_table):
        super().__init__(table_name, ds, source_table, target_table)
        self.primary_key = "id"

    def transform(self):
        if self.df is not None:
            logger.info(f"[{self.table_name}] 4. Specific Transform: Chuẩn hóa dữ liệu Order Items")
            self.df = self.df \
                .withColumn("unit_price", col("unit_price").cast(DecimalType(18, 2))) \
                .withColumn("total_price", col("total_price").cast(DecimalType(18, 2)))
        return self

    def _create_table(self):
        self.df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .create()

class DefaultTransformer(BaseIcebergTransformer):
    def transform(self):
        """Default transform does nothing but ensures the class is not abstract"""
        logger.info(f"[{self.table_name}] No specific transform defined, using default.")
        return self

def main():
    if len(sys.argv) < 5:
        print("Usage: transform_table.py <ds> <table_name> <source_table> <target_table>")
        sys.exit(1)

    ds = sys.argv[1]
    table_name = sys.argv[2]
    source_table = sys.argv[3]
    target_table = sys.argv[4]

    registry = {
        "orders": OrdersTransformer,
        "order_items": OrderItemsTransformer,
        'others': DefaultTransformer
    }

    transformer_cls = registry.get(table_name, registry['others'])

    transformer = transformer_cls(table_name, ds, source_table, target_table)
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
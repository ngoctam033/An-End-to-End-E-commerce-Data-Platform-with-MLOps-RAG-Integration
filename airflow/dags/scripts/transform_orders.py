import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, col, trim, upper, current_timestamp, days
from pyspark.sql.types import DecimalType

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iceberg_transformation")

def transform_orders(ds):
    logger.info(f"Bắt đầu quá trình biến đổi dữ liệu cho ngày: {ds}")
    
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName(f"Transform_Orders_Items_{ds}") \
        .getOrCreate()

    try:
        # 1. Đọc dữ liệu từ Raw Zone
        orders_path = f"s3a://datalake/lakehouse/raw/orders/orders_{ds}.parquet"
        logger.info(f"Đang đọc dữ liệu Raw từ: {orders_path}")
        
        df_orders = spark.read.parquet(orders_path)
        
        if df_orders.count() == 0:
            logger.warning("Không tìm thấy dữ liệu.")
            return

        # 2. Xử lý Transform
        df_orders = df_orders \
            .withColumn("order_date", from_utc_timestamp(col("order_date"), "GMT+7")) \
            .withColumn("created_at", from_utc_timestamp(col("created_at"), "GMT+7")) \
            .withColumn("total_price", col("total_price").cast(DecimalType(18, 2))) \
            .withColumn("status", upper(trim(col("status").cast("string")))) \
            .withColumn("processed_at", current_timestamp()) \
            .filter(col("total_price") >= 0)

        # 3. Ghi dữ liệu vào Silver Zone
        # Với Hadoop Catalog, 'iceberg.silver.orders' sẽ tương ứng với đường dẫn:
        # s3a://datalake/warehouse/silver/orders/
        table_name = "iceberg.silver.orders"

        logger.info(f"Đang ghi dữ liệu vào bảng Iceberg: {table_name}")
        
        # Sử dụng createOrReplace để tự động xử lý nếu đã có folder cũ
        df_orders.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("order_date")) \
            .createOrReplace()

        logger.info("Hoàn thành! Dữ liệu đã được đồng bộ trực tiếp vào MinIO.")

    except Exception as e:
        logger.error(f"Lỗi: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        from datetime import datetime
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    transform_orders(date_str)
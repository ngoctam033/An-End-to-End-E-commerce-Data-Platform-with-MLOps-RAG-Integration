import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, current_timestamp, lit,
    when, datediff, to_date, round as spark_round, coalesce,
    countDistinct
)
from pyspark.sql.types import DecimalType

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mart_return_analysis")


class ReturnAnalysisMartBuilder:
    """
    Gold Mart: Return Analysis
    Phân tích tỉ lệ trả hàng, lý do trả hàng, tác động hoàn tiền theo sản phẩm và danh mục.

    Source Silver Tables:
        - order_return: Thông tin trả hàng (reason, return_type, status, refund_amount, resolved_at)
        - orders: Đơn hàng gốc (customer_id, total_price, order_date, order_channel_id)
        - product: Thông tin sản phẩm (name, sub_category_id)
        - order_items: Chi tiết item trong đơn hàng (product_id, quantity, amount)
        - sub_category, category: Phân loại sản phẩm
    """

    def __init__(self, ds, order_return_table, orders_table, product_table, target_table):
        self.ds = ds
        self.order_return_table = order_return_table
        self.orders_table = orders_table
        self.product_table = product_table
        self.target_table = target_table

        spark_conf = SparkConf()

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
            'spark.cores.max': '2',
            'spark.executor.cores': '1',
            'spark.executor.memory': '1g',
            'spark.driver.memory': '512m',
            'spark.executor.memoryOverhead': '256m',
            'spark.sql.shuffle.partitions': '2',
            'spark.default.parallelism': '2',
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
            .appName(f"DataMart_ReturnAnalysis_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()

    def _read_silver(self, table_name):
        """Read a Silver-layer Iceberg table."""
        full_name = f"iceberg.silver.{table_name}"
        logger.info(f"[ReturnAnalysis] Reading silver table: {full_name}")
        return self.spark.table(full_name)

    def build(self):
        logger.info("[ReturnAnalysis] Building Return Analysis Mart")

        # --- Read source tables ---
        order_return = self.spark.table(self.order_return_table)
        orders = self.spark.table(self.orders_table)
        product = self.spark.table(self.product_table)
        order_items = self._read_silver("order_items")
        sub_category = self._read_silver("sub_category")
        category = self._read_silver("category")

        # --- Join order_return → orders để lấy order context ---
        return_with_order = order_return.alias("ret").join(
            orders.alias("o"),
            col("ret.order_id") == col("o.id"),
            "left"
        )

        # --- Join order_return → order_items → product → category hierarchy ---
        # Mỗi return liên kết tới 1 order, mỗi order có nhiều items
        return_items = return_with_order.join(
            order_items.alias("oi"),
            col("ret.order_id") == col("oi.order_id"),
            "left"
        ).join(
            product.alias("p"),
            col("oi.product_id") == col("p.id"),
            "left"
        ).join(
            sub_category.alias("sc"),
            col("p.sub_category_id") == col("sc.id"),
            "left"
        ).join(
            category.alias("cat"),
            col("sc.category_id") == col("cat.id"),
            "left"
        )

        # --- Aggregate by product & category ---
        result = return_items.groupBy(
            col("p.id").alias("product_id"),
            col("p.name").alias("product_name"),
            col("cat.name").alias("category_name"),
            col("sc.name").alias("sub_category_name")
        ).agg(
            # Return volume
            countDistinct("ret.id").alias("total_returns"),
            countDistinct("ret.order_id").alias("distinct_orders_returned"),

            # Return type breakdown
            spark_sum(when(col("ret.return_type") == "refund", 1).otherwise(0)).alias("refund_count"),
            spark_sum(when(col("ret.return_type") == "exchange", 1).otherwise(0)).alias("exchange_count"),
            spark_sum(when(col("ret.return_type") == "repair", 1).otherwise(0)).alias("repair_count"),

            # Return status funnel
            spark_sum(when(col("ret.status") == "pending", 1).otherwise(0)).alias("status_pending"),
            spark_sum(when(col("ret.status") == "approved", 1).otherwise(0)).alias("status_approved"),
            spark_sum(when(col("ret.status") == "rejected", 1).otherwise(0)).alias("status_rejected"),
            spark_sum(when(col("ret.status") == "processing", 1).otherwise(0)).alias("status_processing"),
            spark_sum(when(col("ret.status") == "completed", 1).otherwise(0)).alias("status_completed"),

            # Refund impact
            spark_sum(coalesce(col("ret.refund_amount"), lit(0))).cast(DecimalType(18, 2)).alias("total_refund_amount"),
            spark_round(avg(coalesce(col("ret.refund_amount"), lit(0))), 2).cast(DecimalType(18, 2)).alias("avg_refund_amount"),

            # Resolution time (days between created_at and resolved_at for completed returns)
            spark_round(
                avg(
                    when(
                        col("ret.resolved_at").isNotNull(),
                        datediff(col("ret.resolved_at"), col("ret.created_at"))
                    )
                ), 1
            ).alias("avg_resolution_days"),

            # Quantity returned (from order_items linked to returned orders)
            spark_sum(coalesce(col("oi.quantity"), lit(0))).alias("total_quantity_in_returned_orders"),

            # Revenue of returned orders
            spark_sum(coalesce(col("oi.amount"), lit(0))).cast(DecimalType(18, 2)).alias("total_revenue_in_returned_orders"),

            # Top return reasons — count by reason
            count(col("ret.reason")).alias("total_reason_entries")
        )

        # --- Calculate total orders per product for return rate ---
        total_orders_per_product = order_items.groupBy("product_id").agg(
            countDistinct("order_id").alias("total_product_orders")
        )

        result = result.join(
            total_orders_per_product.alias("top"),
            col("product_id") == col("top.product_id"),
            "left"
        ).withColumn(
            "return_rate",
            when(
                col("total_product_orders") > 0,
                spark_round(col("distinct_orders_returned") / col("total_product_orders"), 4)
            ).otherwise(0.0)
        ).drop(col("top.product_id"))

        # --- Return reasons distribution (top reasons per product) ---
        reason_dist = order_return.groupBy("order_id").agg(
            count("id").alias("_cnt")  # placeholder, we mainly need the reason
        )
        # Tính top reasons ở cấp tổng thể
        top_reasons = order_return.groupBy("reason").agg(
            count("id").alias("reason_count")
        ).orderBy(col("reason_count").desc())

        # Log top reasons
        logger.info("[ReturnAnalysis] Top return reasons computed")

        # --- Add metadata ---
        result = result.withColumn("processed_at", current_timestamp()) \
                       .withColumn("ds", lit(self.ds))

        logger.info(f"[ReturnAnalysis] Mart built successfully")
        return result

    def _write(self, df):
        """Write result to Iceberg Gold table."""
        if df is None or len(df.take(1)) == 0:
            logger.warning("[ReturnAnalysis] No data to write. Skipping.")
            return

        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[ReturnAnalysis] Creating new table: {self.target_table}")
            df.writeTo(self.target_table) \
                .tableProperty("format-version", "2") \
                .partitionedBy("category_name") \
                .create()
        else:
            logger.info(f"[ReturnAnalysis] Table exists. Overwriting partitions...")
            df.writeTo(self.target_table).overwritePartitions()

        logger.info("[ReturnAnalysis] Write complete.")

    def run(self):
        """Execute full pipeline: build → write."""
        try:
            logger.info("[ReturnAnalysis] === Starting Return Analysis Mart Build ===")
            result_df = self.build()
            self._write(result_df)
            logger.info("[ReturnAnalysis] === Return Analysis Mart Build Complete ===")
        except Exception as e:
            logger.error(f"[ReturnAnalysis] Pipeline error: {str(e)}")
            raise
        finally:
            if self.spark:
                logger.info("[ReturnAnalysis] Stopping Spark Session.")
                self.spark.stop()


def main():
    if len(sys.argv) < 5:
        print("Usage: build_return_analysis_mart.py <ds> <order_return_table> <orders_table> <product_table> <target_table>")
        sys.exit(1)

    ds = sys.argv[1]
    order_return_table = sys.argv[2]
    orders_table = sys.argv[3]
    product_table = sys.argv[4]
    target_table = sys.argv[5]

    builder = ReturnAnalysisMartBuilder(ds, order_return_table, orders_table, product_table, target_table)
    builder.run()


if __name__ == "__main__":
    main()

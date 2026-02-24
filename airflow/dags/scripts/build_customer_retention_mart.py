import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, lit, when, datediff, current_date, to_date,
    round as spark_round, coalesce, countDistinct
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mart_customer_retention")


class CustomerRetentionMartBuilder:
    """
    Gold Mart: Customer Retention
    Phân tích churn, engagement, tần suất mua hàng, và hành vi khách hàng.

    Source Silver Tables:
        - customer_activity_log: Lịch sử hoạt động (view, search, add_to_cart, purchase, ...)
        - orders: Đơn hàng (customer_id, total_price, order_date, status)
        - customers: Thông tin khách hàng (name, email, tier, loyalty_points)
        - cart: Giỏ hàng (status: active, checked_out, abandoned, expired)
    """

    def __init__(self, ds, activity_log_table, orders_table, target_table):
        self.ds = ds
        self.activity_log_table = activity_log_table
        self.orders_table = orders_table
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
            .appName(f"DataMart_CustomerRetention_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()

    def _read_silver(self, table_name):
        """Read a Silver-layer Iceberg table."""
        full_name = f"iceberg.silver.{table_name}"
        logger.info(f"[CustomerRetention] Reading silver table: {full_name}")
        return self.spark.table(full_name)

    def build(self):
        logger.info("[CustomerRetention] Building Customer Retention Mart")

        # --- Read source tables ---
        activity_log = self.spark.table(self.activity_log_table)
        orders = self.spark.table(self.orders_table)
        customers = self._read_silver("customers")
        cart = self._read_silver("cart")

        # =====================================================================
        # 1. Order-based retention metrics per customer
        # =====================================================================
        order_metrics = orders.groupBy("customer_id").agg(
            count("id").alias("total_orders"),
            spark_sum("total_price").cast(DecimalType(18, 2)).alias("total_spent"),
            spark_round(avg("total_price"), 2).cast(DecimalType(18, 2)).alias("avg_order_value"),
            spark_min(to_date("order_date")).alias("first_order_date"),
            spark_max(to_date("order_date")).alias("last_order_date"),

            # Completed / Cancelled counts
            spark_sum(when(col("status") == "Completed", 1).otherwise(0)).alias("completed_orders"),
            spark_sum(when(col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders"),
            spark_sum(when(col("status") == "Returned", 1).otherwise(0)).alias("returned_orders"),

            # Distinct months with orders → purchase frequency proxy
            countDistinct(
                to_date("order_date")
            ).alias("distinct_order_dates")
        )

        # =====================================================================
        # 2. Activity-based engagement metrics per customer
        # =====================================================================
        activity_metrics = activity_log.groupBy("customer_id").agg(
            count("id").alias("total_activities"),

            # Activity type distribution
            spark_sum(when(col("activity_type") == "view", 1).otherwise(0)).alias("view_count"),
            spark_sum(when(col("activity_type") == "search", 1).otherwise(0)).alias("search_count"),
            spark_sum(when(col("activity_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            spark_sum(when(col("activity_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_from_cart_count"),
            spark_sum(when(col("activity_type") == "purchase", 1).otherwise(0)).alias("purchase_activity_count"),
            spark_sum(when(col("activity_type") == "review", 1).otherwise(0)).alias("review_count"),
            spark_sum(when(col("activity_type") == "wishlist", 1).otherwise(0)).alias("wishlist_count"),

            # Distinct products interacted with
            countDistinct("product_id").alias("distinct_products_interacted"),

            # Distinct channels used
            countDistinct("channel").alias("distinct_channels_used"),

            # Last activity date
            spark_max(to_date("created_at")).alias("last_activity_date")
        )

        # =====================================================================
        # 3. Cart abandonment metrics per customer
        # =====================================================================
        cart_metrics = cart.groupBy("customer_id").agg(
            count("id").alias("total_carts"),
            spark_sum(when(col("status") == "checked_out", 1).otherwise(0)).alias("checked_out_carts"),
            spark_sum(when(col("status") == "abandoned", 1).otherwise(0)).alias("abandoned_carts"),
            spark_sum(when(col("status") == "expired", 1).otherwise(0)).alias("expired_carts"),
            spark_sum(when(col("status") == "active", 1).otherwise(0)).alias("active_carts"),

            spark_sum(coalesce("total_amount", lit(0))).cast(DecimalType(18, 2)).alias("total_cart_value"),
            spark_sum(
                when(col("status") == "abandoned", col("total_amount")).otherwise(0)
            ).cast(DecimalType(18, 2)).alias("abandoned_cart_value")
        )

        # =====================================================================
        # 4. Join all metrics → customers
        # =====================================================================
        result = customers.alias("c").join(
            order_metrics.alias("om"),
            col("c.id") == col("om.customer_id"),
            "left"
        ).join(
            activity_metrics.alias("am"),
            col("c.id") == col("am.customer_id"),
            "left"
        ).join(
            cart_metrics.alias("cm"),
            col("c.id") == col("cm.customer_id"),
            "left"
        ).select(
            col("c.id").alias("customer_id"),
            col("c.name").alias("customer_name"),
            col("c.email"),
            col("c.tier"),
            col("c.loyalty_points"),

            # Order metrics
            coalesce(col("om.total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("om.total_spent"), lit(0)).cast(DecimalType(18, 2)).alias("total_spent"),
            coalesce(col("om.avg_order_value"), lit(0)).cast(DecimalType(18, 2)).alias("avg_order_value"),
            col("om.first_order_date"),
            col("om.last_order_date"),
            coalesce(col("om.completed_orders"), lit(0)).alias("completed_orders"),
            coalesce(col("om.cancelled_orders"), lit(0)).alias("cancelled_orders"),
            coalesce(col("om.returned_orders"), lit(0)).alias("returned_orders"),
            coalesce(col("om.distinct_order_dates"), lit(0)).alias("distinct_order_dates"),

            # Activity metrics
            coalesce(col("am.total_activities"), lit(0)).alias("total_activities"),
            coalesce(col("am.view_count"), lit(0)).alias("view_count"),
            coalesce(col("am.search_count"), lit(0)).alias("search_count"),
            coalesce(col("am.add_to_cart_count"), lit(0)).alias("add_to_cart_count"),
            coalesce(col("am.purchase_activity_count"), lit(0)).alias("purchase_activity_count"),
            coalesce(col("am.review_count"), lit(0)).alias("review_count"),
            coalesce(col("am.wishlist_count"), lit(0)).alias("wishlist_activity_count"),
            coalesce(col("am.distinct_products_interacted"), lit(0)).alias("distinct_products_interacted"),
            coalesce(col("am.distinct_channels_used"), lit(0)).alias("distinct_channels_used"),
            col("am.last_activity_date"),

            # Cart metrics
            coalesce(col("cm.total_carts"), lit(0)).alias("total_carts"),
            coalesce(col("cm.checked_out_carts"), lit(0)).alias("checked_out_carts"),
            coalesce(col("cm.abandoned_carts"), lit(0)).alias("abandoned_carts"),
            coalesce(col("cm.abandoned_cart_value"), lit(0)).cast(DecimalType(18, 2)).alias("abandoned_cart_value")
        )

        # =====================================================================
        # 5. Derived metrics
        # =====================================================================

        # Days since last order
        result = result.withColumn(
            "days_since_last_order",
            when(col("last_order_date").isNotNull(),
                 datediff(current_date(), col("last_order_date"))
            ).otherwise(lit(None))
        )

        # Days since last activity
        result = result.withColumn(
            "days_since_last_activity",
            when(col("last_activity_date").isNotNull(),
                 datediff(current_date(), col("last_activity_date"))
            ).otherwise(lit(None))
        )

        # Churn risk segmentation based on days since last order
        result = result.withColumn(
            "churn_segment",
            when(col("total_orders") == 0, "Never Purchased")
            .when(col("days_since_last_order") <= 30, "Active")
            .when(col("days_since_last_order") <= 90, "At Risk")
            .when(col("days_since_last_order") <= 180, "Dormant")
            .otherwise("Churned")
        )

        # Repeat purchase flag
        result = result.withColumn(
            "is_repeat_customer",
            when(col("total_orders") > 1, True).otherwise(False)
        )

        # Avg days between orders (customer lifetime / number of orders)
        result = result.withColumn(
            "avg_days_between_orders",
            when(
                col("total_orders") > 1,
                spark_round(
                    datediff(col("last_order_date"), col("first_order_date")) / (col("total_orders") - 1),
                    1
                )
            ).otherwise(lit(None))
        )

        # Cart abandonment rate per customer
        result = result.withColumn(
            "cart_abandonment_rate",
            when(
                col("total_carts") > 0,
                spark_round(col("abandoned_carts") / col("total_carts") * 100, 2)
            ).otherwise(0.0)
        )

        # Engagement score (composite: views + searches + add_to_cart + purchases + reviews + wishlists)
        # Weighted score: purchase=5, add_to_cart=3, review=3, view=1, search=1, wishlist=2
        result = result.withColumn(
            "engagement_score",
            (col("purchase_activity_count") * 5 +
             col("add_to_cart_count") * 3 +
             col("review_count") * 3 +
             col("wishlist_activity_count") * 2 +
             col("view_count") * 1 +
             col("search_count") * 1)
        )

        # Add metadata
        result = result.withColumn("processed_at", current_timestamp()) \
                       .withColumn("ds", lit(self.ds))

        logger.info("[CustomerRetention] Mart built successfully")
        return result

    def _write(self, df):
        """Write result to Iceberg Gold table."""
        if df is None or len(df.take(1)) == 0:
            logger.warning("[CustomerRetention] No data to write. Skipping.")
            return

        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[CustomerRetention] Creating new table: {self.target_table}")
            df.writeTo(self.target_table) \
                .tableProperty("format-version", "2") \
                .partitionedBy("churn_segment") \
                .create()
        else:
            logger.info("[CustomerRetention] Table exists. Overwriting partitions...")
            df.writeTo(self.target_table).overwritePartitions()

        logger.info("[CustomerRetention] Write complete.")

    def run(self):
        """Execute full pipeline: build → write."""
        try:
            logger.info("[CustomerRetention] === Starting Customer Retention Mart Build ===")
            result_df = self.build()
            self._write(result_df)
            logger.info("[CustomerRetention] === Customer Retention Mart Build Complete ===")
        except Exception as e:
            logger.error(f"[CustomerRetention] Pipeline error: {str(e)}")
            raise
        finally:
            if self.spark:
                logger.info("[CustomerRetention] Stopping Spark Session.")
                self.spark.stop()


def main():
    if len(sys.argv) < 4:
        print("Usage: build_customer_retention_mart.py <ds> <activity_log_table> <orders_table> <target_table>")
        sys.exit(1)

    ds = sys.argv[1]
    activity_log_table = sys.argv[2]
    orders_table = sys.argv[3]
    target_table = sys.argv[4]

    builder = CustomerRetentionMartBuilder(ds, activity_log_table, orders_table, target_table)
    builder.run()


if __name__ == "__main__":
    main()

import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, current_date, lit, when, datediff, to_date,
    round as spark_round, coalesce, abs as spark_abs
)
from pyspark.sql.types import DecimalType

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mart_inventory_analysis")


class InventoryAnalysisMartBuilder:
    """
    Gold Mart: Inventory Analysis
    Phân tích tồn kho: mức stock, turnover rate, dead stock, hao hụt, và so sánh kho hàng.

    Source Silver Tables:
        - inventory: Thông tin tồn kho (product_id, warehouse_id, quantity, safety_stock, reorder_level, unit_cost)
        - inventory_log: Lịch sử thay đổi (change_type: sale/restock/return/adjustment/damage, quantity_change)
        - warehouse: Thông tin kho hàng (name, address, geo_location_id, capacity_sqm)
        - product: Thông tin sản phẩm (name, sub_category_id, price)
        - sub_category, category: Phân loại sản phẩm
    """

    def __init__(self, ds, inventory_table, inventory_log_table, warehouse_table, product_table, target_table):
        self.ds = ds
        self.inventory_table = inventory_table
        self.inventory_log_table = inventory_log_table
        self.warehouse_table = warehouse_table
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
            .appName(f"DataMart_InventoryAnalysis_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()

    def _read_silver(self, table_name):
        """Read a Silver-layer Iceberg table."""
        full_name = f"iceberg.silver.{table_name}"
        logger.info(f"[InventoryAnalysis] Reading silver table: {full_name}")
        return self.spark.table(full_name)

    def build(self):
        logger.info("[InventoryAnalysis] Building Inventory Analysis Mart")

        # --- Read source tables ---
        inventory = self.spark.table(self.inventory_table)
        inv_log = self.spark.table(self.inventory_log_table)
        warehouse = self.spark.table(self.warehouse_table)
        product = self.spark.table(self.product_table)
        sub_category = self._read_silver("sub_category")
        category = self._read_silver("category")
        geo = self._read_silver("geo_location")

        # =====================================================================
        # 1. Inventory log aggregation per inventory_id
        # =====================================================================
        log_metrics = inv_log.groupBy("inventory_id").agg(
            # Volume by change type (quantity_change: negative for sale, positive for restock)
            spark_sum(
                when(col("change_type") == "sale", spark_abs(col("quantity_change"))).otherwise(0)
            ).alias("total_sold_qty"),

            spark_sum(
                when(col("change_type") == "restock", col("quantity_change")).otherwise(0)
            ).alias("total_restock_qty"),

            spark_sum(
                when(col("change_type") == "return", col("quantity_change")).otherwise(0)
            ).alias("total_return_qty"),

            spark_sum(
                when(col("change_type") == "adjustment", col("quantity_change")).otherwise(0)
            ).alias("total_adjustment_qty"),

            spark_sum(
                when(col("change_type") == "damage", spark_abs(col("quantity_change"))).otherwise(0)
            ).alias("total_damage_qty"),

            # Count of each change type
            spark_sum(when(col("change_type") == "sale", 1).otherwise(0)).alias("sale_event_count"),
            spark_sum(when(col("change_type") == "restock", 1).otherwise(0)).alias("restock_event_count"),
            spark_sum(when(col("change_type") == "damage", 1).otherwise(0)).alias("damage_event_count"),

            # Total events
            count("id").alias("total_log_events"),

            # Last activity on this inventory item
            spark_max(to_date("created_at")).alias("last_movement_date"),

            # First activity
            spark_min(to_date("created_at")).alias("first_movement_date")
        )

        # =====================================================================
        # 2. Build product hierarchy
        # =====================================================================
        product_hierarchy = product.alias("p").join(
            sub_category.alias("sc"),
            col("p.sub_category_id") == col("sc.id"),
            "left"
        ).join(
            category.alias("cat"),
            col("sc.category_id") == col("cat.id"),
            "left"
        ).select(
            col("p.id").alias("product_id"),
            col("p.name").alias("product_name"),
            col("p.price").alias("product_price"),
            col("sc.name").alias("sub_category_name"),
            col("cat.name").alias("category_name")
        )

        # =====================================================================
        # 3. Join inventory → warehouse → geo → product → log_metrics
        # =====================================================================
        result = inventory.alias("inv").join(
            warehouse.alias("w"),
            col("inv.warehouse_id") == col("w.id"),
            "left"
        ).join(
            geo.alias("g"),
            col("w.geo_location_id") == col("g.ward_code"),
            "left"
        ).join(
            product_hierarchy.alias("ph"),
            col("inv.product_id") == col("ph.product_id"),
            "left"
        ).join(
            log_metrics.alias("lm"),
            col("inv.id") == col("lm.inventory_id"),
            "left"
        ).select(
            # Inventory identifiers
            col("inv.id").alias("inventory_id"),
            col("inv.product_id"),
            col("ph.product_name"),
            col("ph.category_name"),
            col("ph.sub_category_name"),
            col("ph.product_price"),

            # Warehouse info
            col("w.id").alias("warehouse_id"),
            col("w.name").alias("warehouse_name"),
            col("g.province_name"),

            # Current stock levels
            col("inv.quantity").alias("current_quantity"),
            col("inv.safety_stock"),
            col("inv.reorder_level"),
            col("inv.unit_cost"),

            # Stock value
            (col("inv.quantity") * col("inv.unit_cost")).cast(DecimalType(18, 2)).alias("stock_value"),

            # Stock alerts
            (col("inv.quantity") <= col("inv.reorder_level")).alias("is_low_stock"),
            (col("inv.quantity") <= col("inv.safety_stock")).alias("is_critical_stock"),
            (col("inv.quantity") == 0).alias("is_out_of_stock"),

            # Stock buffer ratio: how much above safety stock
            when(
                col("inv.safety_stock") > 0,
                spark_round((col("inv.quantity") - col("inv.safety_stock")) / col("inv.safety_stock"), 2)
            ).otherwise(lit(None)).alias("stock_buffer_ratio"),

            # Log-based metrics
            coalesce(col("lm.total_sold_qty"), lit(0)).alias("total_sold_qty"),
            coalesce(col("lm.total_restock_qty"), lit(0)).alias("total_restock_qty"),
            coalesce(col("lm.total_return_qty"), lit(0)).alias("total_return_qty"),
            coalesce(col("lm.total_adjustment_qty"), lit(0)).alias("total_adjustment_qty"),
            coalesce(col("lm.total_damage_qty"), lit(0)).alias("total_damage_qty"),

            coalesce(col("lm.sale_event_count"), lit(0)).alias("sale_event_count"),
            coalesce(col("lm.restock_event_count"), lit(0)).alias("restock_event_count"),
            coalesce(col("lm.damage_event_count"), lit(0)).alias("damage_event_count"),
            coalesce(col("lm.total_log_events"), lit(0)).alias("total_log_events"),

            col("lm.last_movement_date"),
            col("lm.first_movement_date")
        )

        # =====================================================================
        # 4. Derived metrics
        # =====================================================================

        # Days since last movement → dead stock detection
        result = result.withColumn(
            "days_since_last_movement",
            when(col("last_movement_date").isNotNull(),
                 datediff(current_date(), col("last_movement_date"))
            ).otherwise(lit(None))
        )

        # Dead stock flag (no sales in 30+ days and still has quantity)
        result = result.withColumn(
            "is_dead_stock",
            when(
                (col("current_quantity") > 0) &
                (
                    (col("days_since_last_movement") > 30) |
                    (col("days_since_last_movement").isNull())
                ),
                True
            ).otherwise(False)
        )

        # Inventory turnover rate = total_sold_qty / current_quantity
        # (Higher = products selling faster, Lower = slow-moving inventory)
        result = result.withColumn(
            "turnover_rate",
            when(
                col("current_quantity") > 0,
                spark_round(col("total_sold_qty") / col("current_quantity"), 2)
            ).otherwise(0.0)
        )

        # Damage/loss rate = total_damage_qty / (total_sold_qty + current_quantity)
        result = result.withColumn(
            "damage_loss_rate",
            when(
                (col("total_sold_qty") + col("current_quantity")) > 0,
                spark_round(
                    col("total_damage_qty") / (col("total_sold_qty") + col("current_quantity")) * 100,
                    2
                )
            ).otherwise(0.0)
        )

        # Days of stock remaining (at current sell rate)
        # avg daily sales = total_sold_qty / days between first and last movement
        result = result.withColumn(
            "days_of_stock_remaining",
            when(
                (col("total_sold_qty") > 0) &
                (col("first_movement_date").isNotNull()) &
                (col("last_movement_date").isNotNull()) &
                (datediff(col("last_movement_date"), col("first_movement_date")) > 0),
                spark_round(
                    col("current_quantity") / (
                        col("total_sold_qty") / datediff(col("last_movement_date"), col("first_movement_date"))
                    ),
                    1
                )
            ).otherwise(lit(None))
        )

        # Snapshot date and metadata
        result = result.withColumn("snapshot_date", current_date()) \
                       .withColumn("processed_at", current_timestamp()) \
                       .withColumn("ds", lit(self.ds))

        logger.info("[InventoryAnalysis] Mart built successfully")
        return result

    def _write(self, df):
        """Write result to Iceberg Gold table."""
        from pyspark.sql.functions import days

        if df is None or len(df.take(1)) == 0:
            logger.warning("[InventoryAnalysis] No data to write. Skipping.")
            return

        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[InventoryAnalysis] Creating new table: {self.target_table}")
            df.writeTo(self.target_table) \
                .tableProperty("format-version", "2") \
                .partitionedBy(days("snapshot_date")) \
                .create()
        else:
            logger.info("[InventoryAnalysis] Table exists. Overwriting partitions...")
            df.writeTo(self.target_table).overwritePartitions()

        logger.info("[InventoryAnalysis] Write complete.")

    def run(self):
        """Execute full pipeline: build → write."""
        try:
            logger.info("[InventoryAnalysis] === Starting Inventory Analysis Mart Build ===")
            result_df = self.build()
            self._write(result_df)
            logger.info("[InventoryAnalysis] === Inventory Analysis Mart Build Complete ===")
        except Exception as e:
            logger.error(f"[InventoryAnalysis] Pipeline error: {str(e)}")
            raise
        finally:
            if self.spark:
                logger.info("[InventoryAnalysis] Stopping Spark Session.")
                self.spark.stop()


def main():
    if len(sys.argv) < 6:
        print("Usage: build_inventory_analysis_mart.py <ds> <inventory_table> <inventory_log_table> <warehouse_table> <product_table> <target_table>")
        sys.exit(1)

    ds = sys.argv[1]
    inventory_table = sys.argv[2]
    inventory_log_table = sys.argv[3]
    warehouse_table = sys.argv[4]
    product_table = sys.argv[5]
    target_table = sys.argv[6]

    builder = InventoryAnalysisMartBuilder(ds, inventory_table, inventory_log_table, warehouse_table, product_table, target_table)
    builder.run()


if __name__ == "__main__":
    main()

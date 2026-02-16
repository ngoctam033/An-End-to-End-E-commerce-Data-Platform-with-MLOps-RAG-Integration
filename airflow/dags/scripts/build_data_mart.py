import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, lit, when, datediff, current_date, to_date,
    round as spark_round, coalesce, to_json, struct, collect_list
)
from pyspark.sql.types import DecimalType
from abc import ABC, abstractmethod

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_mart_builder")


class BaseDataMartBuilder(ABC):
    """Base class for building Gold-layer data marts from Silver Iceberg tables."""

    def __init__(self, mart_name, ds, target_table):
        self.mart_name = mart_name
        self.ds = ds
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
            .appName(f"DataMart_{mart_name}_{ds}") \
            .config(conf=spark_conf) \
            .getOrCreate()

    def _read_silver(self, table_name):
        """Read a Silver-layer Iceberg table."""
        full_name = f"iceberg.silver.{table_name}"
        logger.info(f"[{self.mart_name}] Reading silver table: {full_name}")
        return self.spark.table(full_name)

    def _prepare_target(self):
        """Create namespace if not exists."""
        namespace = ".".join(self.target_table.split(".")[:-1])
        if namespace:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    @abstractmethod
    def _create_table(self, df):
        """Create the Iceberg table with appropriate partitioning."""
        pass

    @abstractmethod
    def build(self):
        """Build the data mart. Returns a DataFrame."""
        pass

    def _write(self, df):
        """Write the result DataFrame to the target Iceberg table."""
        if df is None or df.count() == 0:
            logger.warning(f"[{self.mart_name}] No data to write. Skipping.")
            return

        self._prepare_target()

        if not self.spark.catalog.tableExists(self.target_table):
            logger.info(f"[{self.mart_name}] Creating new table: {self.target_table}")
            self._create_table(df)
        else:
            logger.info(f"[{self.mart_name}] Table exists. Overwriting data...")
            # For marts, we overwrite the entire table to ensure consistency
            df.writeTo(self.target_table).overwritePartitions()

        logger.info(f"[{self.mart_name}] Write complete. Rows: {df.count()}")

    def run(self):
        """Execute the full pipeline: build → write."""
        try:
            logger.info(f"[{self.mart_name}] === Starting Data Mart Build ===")
            result_df = self.build()
            self._write(result_df)
            logger.info(f"[{self.mart_name}] === Data Mart Build Complete ===")
        except Exception as e:
            logger.error(f"[{self.mart_name}] Pipeline error: {str(e)}")
            raise
        finally:
            if self.spark:
                logger.info(f"[{self.mart_name}] Stopping Spark Session.")
                self.spark.stop()


# =============================================================================
# 1. Daily Sales Mart
# =============================================================================
class DailySalesMartBuilder(BaseDataMartBuilder):
    """
    Aggregates daily sales metrics by date, province, and order channel.
    Sources: orders, order_items, customers, geo_location, order_return
    """

    def _create_table(self, df):
        from pyspark.sql.functions import days
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("order_date")) \
            .create()

    def build(self):
        logger.info(f"[{self.mart_name}] Building Daily Sales Mart")

        orders = self._read_silver("orders")
        order_items = self._read_silver("order_items")
        customers = self._read_silver("customers")
        geo = self._read_silver("geo_location")
        order_return = self._read_silver("order_return")

        # Join orders with order_items to get item-level details
        order_with_items = orders.alias("o").join(
            order_items.alias("oi"),
            col("o.id") == col("oi.order_id"),
            "left"
        )

        # Join with customers → geo_location for province
        order_with_geo = order_with_items.join(
            customers.alias("c"),
            col("o.customer_id") == col("c.id"),
            "left"
        ).join(
            geo.alias("g"),
            col("c.geo_location_id") == col("g.ward_code"),
            "left"
        )

        # Calculate return metrics per order
        return_metrics = order_return.groupBy("order_id").agg(
            count("id").alias("return_count"),
            spark_sum("refund_amount").alias("refund_amount")
        )

        # Join with returns
        final_data = order_with_geo.join(
            return_metrics.alias("r"),
            col("o.id") == col("r.order_id"),
            "left"
        )

        # Aggregate by date, province, channel
        result = final_data.groupBy(
            to_date(col("o.order_date")).alias("order_date"),
            col("g.province_name"),
            col("o.order_channel_id")
        ).agg(
            count(col("o.id")).alias("total_orders"),
            spark_sum(col("o.total_price")).cast(DecimalType(18, 2)).alias("total_revenue"),
            spark_sum(col("o.profit")).cast(DecimalType(18, 2)).alias("total_profit"),
            spark_sum(col("oi.quantity")).alias("total_items_sold"),
            spark_round(avg(col("o.total_price")), 2).cast(DecimalType(18, 2)).alias("avg_order_value"),
            spark_sum(coalesce(col("oi.discount_amount"), lit(0))).cast(DecimalType(18, 2)).alias("total_discount_amount"),
            
            # New metrics v2.0
            spark_sum(when(col("o.status") == 'Completed', 1).otherwise(0)).alias("completed_orders"),
            spark_sum(when(col("o.status") == 'Cancelled', 1).otherwise(0)).alias("cancelled_orders"),
            spark_sum(when(col("o.status") == 'Returned', 1).otherwise(0)).alias("returned_orders"),
            spark_sum(coalesce(col("r.return_count"), lit(0))).alias("total_returns_count"),
            spark_sum(coalesce(col("r.refund_amount"), lit(0))).cast(DecimalType(18, 2)).alias("total_refund_amount")
        ).withColumn("cancellation_rate", 
                     spark_round(col("cancelled_orders") / col("total_orders"), 4)
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))

        logger.info(f"[{self.mart_name}] Daily Sales Mart built with {result.count()} rows")
        return result


# =============================================================================
# 2. Customer Analytics Mart
# =============================================================================
class CustomerAnalyticsMartBuilder(BaseDataMartBuilder):
    """
    Customer-level metrics: total spend, order counts, segmentation.
    Sources: customers, orders, order_items, geo_location, order_return, cart, customer_activity_log
    """

    def _create_table(self, df):
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("tier") \
            .create()

    def build(self):
        logger.info(f"[{self.mart_name}] Building Customer Analytics Mart")

        customers = self._read_silver("customers")
        orders = self._read_silver("orders")
        order_items = self._read_silver("order_items")
        geo = self._read_silver("geo_location")
        order_return = self._read_silver("order_return")
        cart = self._read_silver("cart")
        activity_log = self._read_silver("customer_activity_log")

        # Customer order metrics
        customer_orders = orders.groupBy("customer_id").agg(
            count("id").alias("total_orders"),
            spark_sum("total_price").cast(DecimalType(18, 2)).alias("total_spent"),
            spark_round(avg("total_price"), 2).cast(DecimalType(18, 2)).alias("avg_order_value"),
            spark_min(to_date("order_date")).alias("first_order_date"),
            spark_max(to_date("order_date")).alias("last_order_date")
        )

        # Total items purchased
        items_per_customer = orders.alias("o").join(
            order_items.alias("oi"),
            col("o.id") == col("oi.order_id"),
            "inner"
        ).groupBy(col("o.customer_id")).agg(
            spark_sum(col("oi.quantity")).alias("total_items_purchased")
        )

        # Returns metrics
        returns_per_customer = order_return.groupBy("customer_id").agg(
            count("id").alias("total_returns"),
            spark_sum("refund_amount").cast(DecimalType(18, 2)).alias("total_refund_amount")
        )

        # Cart metrics
        carts_per_customer = cart.groupBy("customer_id").agg(
            count("id").alias("total_carts_created"),
            spark_sum(when(col("status") == 'checked_out', 1).otherwise(0)).alias("total_carts_checked_out")
        )

        # Activity metrics
        activity_per_customer = activity_log.groupBy("customer_id").agg(
            count("id").alias("total_activities")
        )

        # Build final mart
        result = customers.alias("c").join(
            customer_orders.alias("co"),
            col("c.id") == col("co.customer_id"),
            "left"
        ).join(
            items_per_customer.alias("ip"),
            col("c.id") == col("ip.customer_id"),
            "left"
        ).join(
            returns_per_customer.alias("rc"),
            col("c.id") == col("rc.customer_id"),
            "left"
        ).join(
            carts_per_customer.alias("cc"),
            col("c.id") == col("cc.customer_id"),
            "left"
        ).join(
            activity_per_customer.alias("ac"),
            col("c.id") == col("ac.customer_id"),
            "left"
        ).join(
            geo.alias("g"),
            col("c.geo_location_id") == col("g.ward_code"),
            "left"
        ).select(
            col("c.id").alias("customer_id"),
            col("c.name").alias("customer_name"),
            col("c.email"),
            col("c.phone"),
            col("g.province_name"),
            col("c.tier").alias("tier"),
            col("c.loyalty_points"),
            col("co.first_order_date"),
            col("co.last_order_date"),
            coalesce(col("co.total_orders"), lit(0)).alias("total_orders"),
            coalesce(col("co.total_spent"), lit(0)).cast(DecimalType(18, 2)).alias("total_spent"),
            coalesce(col("co.avg_order_value"), lit(0)).cast(DecimalType(18, 2)).alias("avg_order_value"),
            coalesce(col("ip.total_items_purchased"), lit(0)).alias("total_items_purchased"),
            coalesce(col("rc.total_returns"), lit(0)).alias("total_returns"),
            coalesce(col("rc.total_refund_amount"), lit(0)).cast(DecimalType(18, 2)).alias("total_refund_amount"),
            coalesce(col("cc.total_carts_created"), lit(0)).alias("total_carts_created"),
            coalesce(col("cc.total_carts_checked_out"), lit(0)).alias("total_carts_checked_out"),
            coalesce(col("ac.total_activities"), lit(0)).alias("total_activities"),
            datediff(current_date(), col("co.last_order_date")).alias("days_since_last_order")
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))

        logger.info(f"[{self.mart_name}] Customer Analytics Mart built with {result.count()} rows")
        return result


# =============================================================================
# 3. Product Performance Mart
# =============================================================================
class ProductPerformanceMartBuilder(BaseDataMartBuilder):
    """
    Product-level performance metrics with category hierarchy and reviews.
    Sources: order_items, product, sub_category, category, brand, product_review, order_return, wishlist
    """

    def _create_table(self, df):
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("category_name") \
            .create()

    def build(self):
        logger.info(f"[{self.mart_name}] Building Product Performance Mart")

        order_items = self._read_silver("order_items")
        product = self._read_silver("product")
        sub_category = self._read_silver("sub_category")
        category = self._read_silver("category")
        brand = self._read_silver("brand")
        product_review = self._read_silver("product_review")
        order_return = self._read_silver("order_return")
        wishlist = self._read_silver("wishlist")
        
        # We need to join return -> orders -> order_items to find product returns,
        # but order_return is order-level. For simplicity, we can join order_return 
        # to order_items via order_id to estimate or if line-item return is not supported.
        # However, v2 schema has order_return linked to order_id only. 
        # A better approximation: returns usually impact all items or specific ones. 
        # Let's assume proportional impact or just count order-level returns associated with products.
        # BETTER: Join order_items with order_return on order_id. 
        # If an order is returned, all its items are considered returned for this mart's aggregation.
        
        returned_items = order_items.alias("oi").join(
            order_return.alias("or"),
            col("oi.order_id") == col("or.order_id"),
            "inner"
        ).groupBy("oi.product_id").agg(
            count("or.id").alias("return_count")
        )

        # Wishlist count
        wishlist_count = wishlist.where("is_active = true").groupBy("product_id").agg(
            count("id").alias("wishlist_count")
        )

        # Product sales metrics
        product_sales = order_items.groupBy("product_id").agg(
            spark_sum("quantity").alias("total_sold"),
            spark_sum("amount").cast(DecimalType(18, 2)).alias("total_revenue"),
            spark_round(avg("unit_price"), 2).cast(DecimalType(18, 2)).alias("avg_unit_price")
        )

        # Product review metrics
        review_metrics = product_review.groupBy("product_id").agg(
            spark_round(avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating"),
            count("id").alias("total_reviews")
        )

        # Build product hierarchy
        product_hierarchy = product.alias("p").join(
            sub_category.alias("sc"),
            col("p.sub_category_id") == col("sc.id"),
            "left"
        ).join(
            category.alias("cat"),
            col("sc.category_id") == col("cat.id"),
            "left"
        ).join(
            brand.alias("b"),
            col("p.brand_id") == col("b.id"),
            "left"
        ).select(
            col("p.id").alias("product_id"),
            col("p.name").alias("product_name"),
            col("b.name").alias("brand_name"),
            col("cat.name").alias("category_name"),
            col("sc.name").alias("sub_category_name")
        )

        # Join all
        result = product_hierarchy.alias("ph").join(
            product_sales.alias("ps"),
            col("ph.product_id") == col("ps.product_id"),
            "left"
        ).join(
            review_metrics.alias("rm"),
            col("ph.product_id") == col("rm.product_id"),
            "left"
        ).join(
            returned_items.alias("ri"),
            col("ph.product_id") == col("ri.product_id"),
            "left"
        ).join(
            wishlist_count.alias("wl"),
            col("ph.product_id") == col("wl.product_id"),
            "left"
        ).select(
            col("ph.product_id"),
            col("ph.product_name"),
            col("ph.brand_name"),
            col("ph.category_name"),
            col("ph.sub_category_name"),
            coalesce(col("ps.total_sold"), lit(0)).alias("total_sold"),
            coalesce(col("ps.total_revenue"), lit(0)).cast(DecimalType(18, 2)).alias("total_revenue"),
            coalesce(col("ps.avg_unit_price"), lit(0)).cast(DecimalType(18, 2)).alias("avg_unit_price"),
            coalesce(col("rm.avg_rating"), lit(0)).cast(DecimalType(3, 2)).alias("avg_rating"),
            coalesce(col("rm.total_reviews"), lit(0)).alias("total_reviews"),
            coalesce(col("ri.return_count"), lit(0)).alias("total_return_events"),
            coalesce(col("wl.wishlist_count"), lit(0)).alias("total_wishlist_adds")
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))

        # Calculate return rate
        result = result.withColumn(
            "return_rate", 
            when(col("total_sold") > 0, col("total_return_events") / col("total_sold")).otherwise(0.0)
        )

        logger.info(f"[{self.mart_name}] Product Performance Mart built with {result.count()} rows")
        return result


# =============================================================================
# 4. Logistics Mart
# =============================================================================
class LogisticsMartBuilder(BaseDataMartBuilder):
    """
    Shipping & delivery analytics by logistics partner, method, and region.
    Sources: orders, shipment, logistics_partner, shipping_method, geo_location, customers
    """

    def _create_table(self, df):
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("logistics_partner_name") \
            .create()

    def build(self):
        logger.info(f"[{self.mart_name}] Building Logistics Mart")

        orders = self._read_silver("orders")
        shipment = self._read_silver("shipment")
        logistics = self._read_silver("logistics_partner")
        shipping_method = self._read_silver("shipping_method")
        customers = self._read_silver("customers")
        geo = self._read_silver("geo_location")

        # Join shipment with logistics partner and shipping method
        shipment_detail = shipment.alias("s").join(
            logistics.alias("lp"),
            col("s.logistics_partner_id") == col("lp.id"),
            "left"
        ).join(
            shipping_method.alias("sm"),
            col("s.shipping_method_id") == col("sm.id"),
            "left"
        )

        # Join with orders → customers → geo for province
        full_data = orders.alias("o").join(
            shipment_detail.alias("sd"),
            col("o.shipping_id") == col("sd.s.id"),
            "left"
        ).join(
            customers.alias("c"),
            col("o.customer_id") == col("c.id"),
            "left"
        ).join(
            geo.alias("g"),
            col("c.geo_location_id") == col("g.ward_code"),
            "left"
        )
        
        # Calculate delivery time (actual - created) in hours
        # Only for delivered shipments
        full_data = full_data.withColumn(
            "delivery_hours",
            (col("sd.s.actual_delivery").cast("long") - col("sd.s.created_at").cast("long")) / 3600
        ).withColumn(
            "is_on_time",
            when(
                (col("sd.s.actual_delivery").isNotNull()) & 
                (col("sd.s.estimated_delivery").isNotNull()) &
                (col("sd.s.actual_delivery") <= col("sd.s.estimated_delivery")),
                1
            ).otherwise(0)
        )

        # Aggregate
        result = full_data.groupBy(
            col("sd.lp.name").alias("logistics_partner_name"),
            col("sd.sm.name").alias("shipping_method_name"),
            col("g.province_name")
        ).agg(
            count(col("sd.s.id")).alias("total_shipments"),
            spark_sum(col("sd.s.shipping_cost")).cast(DecimalType(18, 2)).alias("total_shipping_cost"),
            spark_round(avg(col("sd.s.shipping_cost")), 2).cast(DecimalType(18, 2)).alias("avg_shipping_cost"),
            spark_round(avg(col("sd.lp.rating")), 2).cast(DecimalType(3, 2)).alias("partner_rating"),
            spark_round(
                spark_sum(when(col("sd.s.is_expedited") == True, 1).otherwise(0)) / count("*") * 100,
                2
            ).cast(DecimalType(5, 2)).alias("expedited_ratio"),
            
            # New metrics v2.0
            spark_round(avg("delivery_hours"), 2).alias("avg_delivery_time_hours"),
            spark_round(spark_sum("is_on_time") / count(col("sd.s.actual_delivery")) * 100, 2).alias("on_time_delivery_rate"),
            spark_round(avg(col("sd.s.delivery_attempts")), 1).alias("avg_delivery_attempts"),
            spark_sum(when(col("sd.s.shipping_status") == 'Failed', 1).otherwise(0)).alias("failed_delivery_count")
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))

        logger.info(f"[{self.mart_name}] Logistics Mart built with {result.count()} rows")
        return result


# =============================================================================
# 5. Inventory Snapshot Mart
# =============================================================================
class InventorySnapshotMartBuilder(BaseDataMartBuilder):
    """
    Current inventory snapshot with stock alerts and turnover metrics.
    Sources: inventory, product, warehouse, geo_location, inventory_log
    """

    def _create_table(self, df):
        from pyspark.sql.functions import days
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy(days("snapshot_date")) \
            .create()

    def build(self):
        logger.info(f"[{self.mart_name}] Building Inventory Snapshot Mart")

        inventory = self._read_silver("inventory")
        product = self._read_silver("product")
        warehouse = self._read_silver("warehouse")
        geo = self._read_silver("geo_location")
        inv_log = self._read_silver("inventory_log")

        # Calculate turnover metrics from logs
        # Since this runs daily, we can aggregate logs for all time or recent window.
        # For snapshot mart, it's often useful to see 'total sales vs stock'
        turnover_metrics = inv_log.groupBy("inventory_id").agg(
            spark_sum(when(col("change_type") == 'sale', col("quantity_change").cast("long")).otherwise(0)).alias("total_outflux"),
            spark_sum(when(col("change_type") == 'restock', col("quantity_change").cast("long")).otherwise(0)).alias("total_influx")
        )

        result = inventory.alias("inv").join(
            product.alias("p"),
            col("inv.product_id") == col("p.id"),
            "left"
        ).join(
            warehouse.alias("w"),
            col("inv.warehouse_id") == col("w.id"),
            "left"
        ).join(
            geo.alias("g"),
            col("w.geo_location_id") == col("g.ward_code"),
            "left"
        ).join(
            turnover_metrics.alias("tm"),
            col("inv.id") == col("tm.inventory_id"),
            "left"
        ).select(
            current_date().alias("snapshot_date"),
            col("w.name").alias("warehouse_name"),
            col("g.province_name"),
            col("inv.product_id"),
            col("p.name").alias("product_name"),
            col("inv.quantity"),
            col("inv.safety_stock"),
            col("inv.reorder_level"),
            (col("inv.quantity") * col("inv.unit_cost")).cast(DecimalType(18, 2)).alias("stock_value"),
            (col("inv.quantity") <= col("inv.reorder_level")).alias("is_low_stock"),
            (col("inv.quantity") == 0).alias("is_out_of_stock"),
            
            # Turnover metrics (negative outflux means reduction)
            coalesce(col("tm.total_outflux"), lit(0)).alias("total_sales_qty_accumulated"),
            coalesce(col("tm.total_influx"), lit(0)).alias("total_restock_qty_accumulated")
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))

        logger.info(f"[{self.mart_name}] Inventory Snapshot Mart built with {result.count()} rows")
        return result


# =============================================================================
# 6. Cart Analytics Mart
# =============================================================================
class CartAnalyticsMartBuilder(BaseDataMartBuilder):
    """
    Cart funnel and abandonment analysis by channel.
    Sources: cart, cart_items, product, customers
    """
    
    def _create_table(self, df):
        from pyspark.sql.functions import days
        df.writeTo(self.target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("channel") \
            .create()
            
    def build(self):
        logger.info(f"[{self.mart_name}] Building Cart Analytics Mart")
        
        cart = self._read_silver("cart")
        
        # Aggregate by channel
        result = cart.groupBy("channel").agg(
            count("id").alias("total_carts"),
            spark_sum(when(col("status") == 'checked_out', 1).otherwise(0)).alias("checked_out_carts"),
            spark_sum(when(col("status") == 'abandoned', 1).otherwise(0)).alias("abandoned_carts"),
            spark_sum(when(col("status") == 'expired', 1).otherwise(0)).alias("expired_carts"),
            spark_sum(when(col("status") == 'active', 1).otherwise(0)).alias("active_carts"),
            
            spark_sum("total_amount").cast(DecimalType(18, 2)).alias("total_cart_value"),
            spark_round(avg("total_amount"), 2).cast(DecimalType(18, 2)).alias("avg_cart_value"),
            spark_round(avg("item_count"), 2).alias("avg_items_per_cart")
        )
        
        # Calculate rates
        result = result.withColumn(
            "cart_abandonment_rate",
            spark_round(col("abandoned_carts") / col("total_carts") * 100, 2)
        ).withColumn(
            "cart_conversion_rate",
            spark_round(col("checked_out_carts") / col("total_carts") * 100, 2)
        ).withColumn("processed_at", current_timestamp()) \
         .withColumn("ds", lit(self.ds))
         
        logger.info(f"[{self.mart_name}] Cart Analytics Mart built with {result.count()} rows")
        return result


# =============================================================================
# Main
# =============================================================================
def main():
    if len(sys.argv) < 4:
        print("Usage: build_data_mart.py <ds> <mart_name> <target_table>")
        sys.exit(1)

    ds, mart_name, target_table = sys.argv[1:4]

    registry = {
        "daily_sales_mart": DailySalesMartBuilder,
        "customer_analytics_mart": CustomerAnalyticsMartBuilder,
        "product_performance_mart": ProductPerformanceMartBuilder,
        "logistics_mart": LogisticsMartBuilder,
        "inventory_snapshot_mart": InventorySnapshotMartBuilder,
        "cart_analytics_mart": CartAnalyticsMartBuilder,
    }

    builder_cls = registry.get(mart_name)
    if builder_cls is None:
        logger.error(f"Unknown mart: {mart_name}. Available: {list(registry.keys())}")
        sys.exit(1)

    builder = builder_cls(mart_name, ds, target_table)
    builder.run()

if __name__ == "__main__":
    main()

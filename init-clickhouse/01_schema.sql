-- ClickHouse Schema
-- Data Warehouse Schema for E-commerce Analytics
-- Optimization: Removed all Nullable types for better performance and to fix sorting key issues.
-- Default values: 0 for numbers, '' for strings, 1970-01-01 for dates.

-- 1. Orders Table
CREATE TABLE orders (
    id UInt64,
    customer_id UInt64,
    payment_id UInt64,
    shipping_id UInt64,
    discount_id UInt64,
    location_id UInt64,
    logistics_partner_id UInt64,
    order_channel_id UInt64,
    order_code String,
    order_date DateTime,
    status String,
    is_active Bool DEFAULT true,
    created_at DateTime,
    updated_at DateTime,
    total_price Decimal(11, 2),
    profit Decimal(12, 2)
) ENGINE = MergeTree()
ORDER BY (id, created_at);

-- 2. Order Items Table
CREATE TABLE order_items (
    id UInt64,
    order_id UInt64,
    product_id UInt64,
    unit_price Decimal(12, 2),
    discount_amount Decimal(12, 2),
    quantity Int32,
    amount Decimal(12, 2),
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY (id, order_id);

-- 3. Order Channel Table
CREATE TABLE order_channel (
    id UInt64,
    name String,
    description String,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 4. Order Status History Table
CREATE TABLE order_status_history (
    id UInt64,
    order_id UInt64,
    status String,
    changed_at DateTime,
    changed_by String,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY (id, order_id);

-- 5. Product Table
CREATE TABLE product (
    id UInt64,
    product_sku String,
    brand_id UInt64,
    name String,
    description String,
    price Decimal(12, 2),
    sub_category_id UInt64,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 6. Product Review Table
CREATE TABLE product_review (
    id UInt64,
    product_id UInt64,
    customer_id UInt64,
    rating Int32,
    review_text String,
    source_system String,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY (product_id, id);

-- 7. Sub Category Table
CREATE TABLE sub_category (
    id UInt64,
    name String,
    description String,
    category_id UInt64,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 8. Category Table
CREATE TABLE category (
    id UInt64,
    name String,
    description String,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 9. Discount Table
CREATE TABLE discount (
    id UInt64,
    name String,
    discount_type String,
    value Decimal(12, 2),
    start_date DateTime,
    end_date DateTime,
    applies_to String,
    source_system String,
    is_active Bool DEFAULT true,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY id;

-- 10. Brand Table
CREATE TABLE brand (
    id UInt64,
    name String,
    country String,
    description String,
    is_active Bool DEFAULT true,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY id;

-- 11. Payment Table
CREATE TABLE payment (
    id UInt64,
    payment_method String,
    payment_status String,
    transaction_id String,
    amount Decimal(12, 2),
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 12. Shipment Table
CREATE TABLE shipment (
    id UInt64,
    logistics_partner_id UInt64,
    warehouse_id UInt64,
    is_expedited Bool DEFAULT false,
    shipping_method_id UInt64,
    tracking_number String,
    shipping_cost Decimal(12, 2),
    shipping_status String,
    created_at DateTime,
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 13. Logistics Partner Table
CREATE TABLE logistics_partner (
    id UInt64,
    name String,
    created_at DateTime,
    rating Decimal(3, 2),
    updated_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 14. Shipping Method Table
CREATE TABLE shipping_method (
    id UInt64,
    name String,
    description String,
    estimated_delivery_time Int32,
    is_active Bool DEFAULT true,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY id;

-- 15. Customers Table
CREATE TABLE customers (
    id UInt64,
    customer_code String,
    geo_location_id UInt64,
    order_channel_id UInt64,
    created_at DateTime,
    is_active Bool DEFAULT true
) ENGINE = MergeTree()
ORDER BY id;

-- 16. Geo Location Table
CREATE TABLE geo_location (
    ward_code UInt64,
    ward_name String,
    ward_type String,
    district_name String,
    province_name String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    is_active Bool DEFAULT true,
    is_before Bool DEFAULT false
) ENGINE = MergeTree()
ORDER BY ward_code;

-- 17. Warehouse Table
CREATE TABLE warehouse (
    id UInt64,
    name String,
    address String,
    geo_location_id UInt64,
    capacity_sqm Decimal(12, 2),
    manager String,
    contact_phone String,
    is_active Bool DEFAULT true,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

-- 18. Inventory Table
CREATE TABLE inventory (
    id UInt64,
    product_id UInt64,
    warehouse_id UInt64,
    quantity Int32,
    safety_stock Int32 DEFAULT 0,
    reorder_level Int32 DEFAULT 0,
    last_counted_at DateTime,
    unit_cost Decimal(12, 2),
    is_active Bool DEFAULT true,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (product_id, warehouse_id);
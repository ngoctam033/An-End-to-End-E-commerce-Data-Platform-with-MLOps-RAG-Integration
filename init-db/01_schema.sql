-- postgresql
-- data warehouse schema for e-commerce analytics

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id BIGINT ,
    payment_id BIGINT ,
    shipping_id BIGINT ,
    discount_id BIGINT,
    location_id BIGINT ,
    logistics_partner_id BIGINT ,
    order_channel_id BIGINT ,
    order_code VARCHAR(50)  UNIQUE NOT NULL,
    order_date TIMESTAMP ,
    status VARCHAR(50) ,
    is_active BOOLEAN  DEFAULT TRUE,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    total_price NUMERIC(11, 2) ,
    profit NUMERIC(12, 2) 
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id BIGINT ,
    product_id BIGINT ,
    unit_price NUMERIC(12, 2) ,
    discount_amount NUMERIC(12, 2),
    quantity INT ,
    amount NUMERIC(12, 2) ,
    is_active BOOLEAN ,
    created_at TIMESTAMP 
);

create table order_channel(
    id serial primary key,
    name VARCHAR(50) ,
    description TEXT,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table order_status_history (
    id serial primary key,
    order_id BIGINT ,
    status VARCHAR(50) ,
    changed_at TIMESTAMP ,
    changed_by VARCHAR(100),
    is_active BOOLEAN  DEFAULT TRUE
);

create table product (
    id serial primary key,
    product_sku VARCHAR(50) ,
    brand_id BIGINT, --,
    name VARCHAR(100) ,
    description TEXT,
    price NUMERIC(12, 2), --,
    sub_category_id BIGINT ,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table product_review (
    id serial primary key,
    product_id BIGINT ,
    customer_id BIGINT ,
    rating INT CHECK (rating >= 1 AND rating <= 5),
    review_text TEXT,
    source_system VARCHAR(50), 
    -- e.g., website nội bộ, sàn thương mại điện tử(e.g., Shopee, Lazada)
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table sub_category (
    id serial primary key,
    name VARCHAR(50) ,
    description TEXT,
    category_id BIGINT ,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table category (
    id serial primary key,
    name VARCHAR(50) ,
    description TEXT,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

CREATE TABLE discount (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    discount_type VARCHAR(50), -- e.g., %, fixed amount
    value NUMERIC(12, 2),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    applies_to VARCHAR(50), -- e.g., product, category, order
    source_system VARCHAR(50), 
    -- e.g., website nội bộ, sàn thương mại điện tử(e.g., Shopee, Lazada)
    is_active BOOLEAN ,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP 
);

CREATE TABLE brand (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) ,
    country VARCHAR(50),
    description TEXT,
    is_active BOOLEAN  DEFAULT TRUE,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP 
);

create table payment (
    id serial primary key,
    payment_method VARCHAR(50) ,
    payment_status VARCHAR(50) ,
    transaction_id VARCHAR(100),
    amount NUMERIC(12, 2) ,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table shipment (
    id serial primary key,
    logistics_partner_id BIGINT ,
    warehouse_id BIGINT ,
    is_expedited BOOLEAN ,
    shipping_method_id BIGINT ,
    tracking_number VARCHAR(100) UNIQUE,
    shipping_cost NUMERIC(12, 2) ,
    shipping_status VARCHAR(50) ,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

-- logistics_partner
create table logistics_partner (
    id serial primary key,
    name VARCHAR(100) ,
    created_at TIMESTAMP ,
    rating numeric(3, 2),
    updated_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

CREATE TABLE shipping_method (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) ,
    description TEXT,
    estimated_delivery_time INT, -- in hours
    is_active BOOLEAN  DEFAULT TRUE,
    created_at TIMESTAMP ,
    updated_at TIMESTAMP 
);

create table customers (
    id serial primary key,
    customer_code VARCHAR(50)  UNIQUE,
    geo_location_id BIGINT ,
    order_channel_id BIGINT ,
    created_at TIMESTAMP ,
    is_active BOOLEAN  DEFAULT TRUE
);

create table geo_location (
    ward_code BIGINT  PRIMARY KEY,
    ward_name VARCHAR(50) ,
    ward_type VARCHAR(50) ,
    district_name VARCHAR(50) ,
    province_name VARCHAR(50) ,
    created_at TIMESTAMP  DEFAULT now(),
    updated_at TIMESTAMP  DEFAULT now(),
    is_active BOOLEAN  DEFAULT TRUE,
    is_before BOOLEAN  DEFAULT FALSE
);
-- Thêm bảng warehouse và inventory
-- Bảng warehouse (kho hàng)
CREATE TABLE warehouse (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) ,
    address TEXT ,
    geo_location_id BIGINT  REFERENCES geo_location(ward_code),
    capacity_sqm NUMERIC(12, 2), -- diện tích theo mét vuông
    manager VARCHAR(100),
    contact_phone VARCHAR(20),
    is_active BOOLEAN  DEFAULT TRUE,
    created_at TIMESTAMP  DEFAULT now(),
    updated_at TIMESTAMP  DEFAULT now()
);

-- Bảng inventory (tồn kho)
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id BIGINT  REFERENCES product(id),
    warehouse_id BIGINT  REFERENCES warehouse(id),
    quantity INT  CHECK (quantity >= 0),
    safety_stock INT  DEFAULT 0, -- lượng tồn kho an toàn tối thiểu
    reorder_level INT  DEFAULT 0, -- mức cần đặt lại hàng
    last_counted_at TIMESTAMP,
    unit_cost NUMERIC(12, 2) , -- giá vốn đơn vị
    is_active BOOLEAN  DEFAULT TRUE,
    created_at TIMESTAMP  DEFAULT now(),
    updated_at TIMESTAMP  DEFAULT now(),
    UNIQUE(product_id, warehouse_id) 
    -- mỗi sản phẩm chỉ có một bản ghi tồn kho trong mỗi kho
);
    
--tạo foreign key cho các bảng
ALTER TABLE orders 
    ADD CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
    add constraint fk_payment FOREIGN KEY (payment_id) REFERENCES payment(id),
    add constraint fk_shipping FOREIGN KEY (shipping_id) REFERENCES shipment(id),
    add constraint fk_discount FOREIGN KEY (discount_id) REFERENCES discount(id),
    add constraint fk_location FOREIGN KEY (location_id) 
        REFERENCES geo_location(ward_code),
    add constraint fk_logistics_partner 
        FOREIGN KEY (logistics_partner_id) REFERENCES logistics_partner(id),
    add constraint fk_order_channel 
        FOREIGN KEY (order_channel_id) REFERENCES order_channel(id);

ALTER TABLE order_items 
    ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id),
    add constraint fk_product FOREIGN KEY (product_id) REFERENCES product(id);

ALTER TABLE order_status_history
    ADD CONSTRAINT fk_order_status_history_order 
        FOREIGN KEY (order_id) REFERENCES orders(id);

ALTER TABLE sub_category
    ADD CONSTRAINT fk_sub_category_category 
        FOREIGN KEY (category_id) REFERENCES category(id);

ALTER TABLE product
    ADD CONSTRAINT fk_product_brand FOREIGN KEY (brand_id) REFERENCES brand(id),
    add constraint fk_product_sub_category 
        FOREIGN KEY (sub_category_id) REFERENCES sub_category(id);

ALTER TABLE customers
    ADD CONSTRAINT fk_customers_geo_location 
        FOREIGN KEY (geo_location_id) REFERENCES geo_location(ward_code);

ALTER TABLE shipment
    ADD CONSTRAINT fk_shipment_logistics_partner 
        FOREIGN KEY (logistics_partner_id) REFERENCES logistics_partner(id);

ALTER TABLE product_review
    ADD CONSTRAINT fk_product_review_product
        FOREIGN KEY (product_id) REFERENCES product(id),
    ADD CONSTRAINT fk_product_review_customer
        FOREIGN KEY (customer_id) REFERENCES customers(id);

-- Chèn dữ liệu từ file CSV vào bảng
COPY geo_location (ward_code, ward_name, ward_type, district_name, province_name)
FROM '/docker-entrypoint-initdb.d/data_geo.csv'
DELIMITER ','
CSV HEADER;

COPY logistics_partner (id, name, created_at, rating, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/logistics_companies.csv'
DELIMITER ','
CSV HEADER;

COPY category (id, name, description, created_at, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/categories.csv'
DELIMITER ','
CSV HEADER;

COPY sub_category (id, name, description, category_id, created_at, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/sub_categories.csv'
DELIMITER ','
CSV HEADER;

COPY order_channel (id, name, description, created_at, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/order_channels.csv'
DELIMITER ','
CSV HEADER;

copy shipping_method (id, name, description, estimated_delivery_time, 
    is_active, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/shipping_methods.csv'
DELIMITER ','
CSV HEADER;

copy brand (id, name, country, description, is_active, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/brand.csv'
DELIMITER ','
CSV HEADER;

copy product (id, product_sku, brand_id, name, description, price, sub_category_id, 
    created_at, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/product.csv'
DELIMITER ','
CSV HEADER;

copy payment (id, payment_method, payment_status, transaction_id, amount,
    created_at, updated_at, is_active)
FROM '/docker-entrypoint-initdb.d/payment.csv'  
DELIMITER ','
CSV HEADER;

copy discount (id, name, discount_type, value, start_date, end_date, is_active,
    created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/discount.csv'
DELIMITER ','
CSV HEADER;

copy warehouse (id, name, address, geo_location_id, capacity_sqm,
    manager, contact_phone, is_active, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/warehouse.csv'
DELIMITER ','
CSV HEADER;

-- Cập nhật tất cả created_at về 2023-01-01 để khớp với Spark Transform logic
UPDATE orders SET created_at = '2023-01-01';
UPDATE order_items SET created_at = '2023-01-01';
UPDATE order_channel SET created_at = '2023-01-01';
UPDATE product SET created_at = '2023-01-01';
UPDATE product_review SET created_at = '2023-01-01';
UPDATE sub_category SET created_at = '2023-01-01';
UPDATE category SET created_at = '2023-01-01';
UPDATE discount SET created_at = '2023-01-01';
UPDATE brand SET created_at = '2023-01-01';
UPDATE payment SET created_at = '2023-01-01';
UPDATE shipment SET created_at = '2023-01-01';
UPDATE logistics_partner SET created_at = '2023-01-01';
UPDATE shipping_method SET created_at = '2023-01-01';
UPDATE customers SET created_at = '2023-01-01';
UPDATE geo_location SET created_at = '2023-01-01';
UPDATE warehouse SET created_at = '2023-01-01';

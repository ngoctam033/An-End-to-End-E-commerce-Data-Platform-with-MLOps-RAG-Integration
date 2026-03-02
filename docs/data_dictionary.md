# Data Dictionary

This document provides a detailed description of the tables in the `iceberg.silver` layer of the E-commerce Data Platform.

## Core Entities

### `orders`
Central table recording all customer transactions.
- **id**: Unique identifier for the order.
- **order_code**: Public-facing order reference (e.g., ORD-2023-...).
- **customer_id**: FK to `customers`.
- **order_date**: Timestamp when the order was placed.
- **status**: Current status (PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED, RETURNED).
- **total_price**: Final amount paid by the customer.
- **created_at**: Benchmark timestamp for ETL processing.
- **confirmed_at**: Timestamp when payment was confirmed.
- **delivered_at**: Timestamp when order was delivered.
- **cancelled_at**: Timestamp when order was cancelled.

### `order_items`
Line items for each order.
- **id**: Unique identifier for the line item.
- **order_id**: FK to `orders`.
- **product_id**: FK to `products`.
- **quantity**: Number of units purchased.
- **unit_price**: Price per unit at purchase time.
- **amount**: Total line amount (`quantity * unit_price`).

### `customers`
Customer profiles and demographics.
- **id**: Key identifier.
- **customer_code**: Unique business key.
- **name**: Customer full name.
- **email**: Contact email.
- **phone**: Contact phone number.
- **tier**: Loyalty tier (Bronze, Silver, Gold, Platinum).
- **loyalty_points**: Current accummulated points.
- **total_spent**: Lifetime gross merchandise value (GMV) from this customer.

### `product`
Product catalog master data.
- **id**: Key identifier.
- **product_sku**: Stock Keeping Unit code.
- **name**: Product name.
- **category_id**: FK to `category`.
- **brand_id**: FK to `brand`.
- **price**: Current list price.

### `inventory`
Current stock levels across warehouses.
- **product_id**: FK to `product`.
- **warehouse_id**: FK to `warehouse`.
- **quantity**: Current on-hand quantity.
- **safety_stock**: Minimum threshold before reorder alert.
- **reorder_level**: Quantity to order when replenishing.

## Business Ops & Logistics

### `order_status_history`
Audit trail of order lifecycle changes.
- **order_id**: FK to `orders`.
- **status**: The status the order changed *to*.
- **changed_at**: Timestamp of the change.
- **changed_by**: User/System actor who made the change.

### `inventory_log`
Audit trail for all stock movements.
- **inventory_id**: FK to `inventory`.
- **change_type**: Reason code (SALE, RESTOCK, RETURN, DAMAGE, ADJUSTMENT).
- **quantity_change**: Delta amount (+/-).
- **quantity_before**: Snapshot before change.
- **quantity_after**: Snapshot after change.
- **reference_id**: Linked Order ID or PO ID.

### `warehouse`
Physical storage locations.
- **id**: Key identifier.
- **name**: Warehouse name.
- **address**: Physical address.
- **capacity_sqm**: Total storage capacity in square meters.

### `shipment`
Logistics and delivery tracking.
- **tracking_number**: Carrier tracking code.
- **logistics_partner_id**: FK to `logistics_partner`.
- **estimated_delivery**: SLA target date.
- **actual_delivery**: Real-world delivery timestamp.

## Customer Experience

### `cart` & `cart_items`
Shopping cart analysis for abandonment and conversion funnels.
- **cart.status**: ACTIVE, ABANDONED, CHECKED_OUT.
- **cart.total_amount**: Value of items in cart.
- **cart.customer_id**: Owner of the cart.

### `wishlist`
Products saved for later.
- **customer_id**: User.
- **product_id**: Product of interest.
- **added_at**: Timestamp of interest.

### `product_review`
User-generated content and ratings.
- **rating**: 1-5 star score.
- **review_text**: Qualitative feedback.
- **customer_id**: Reviewer.
- **product_id**: Reviewed item.

### `order_return`
Post-purchase reverse logistics.
- **order_id**: Original order.
- **reason**: Customer stated reason.
- **return_type**: REFUND, EXCHANGE, REPAIR.
- **status**: PENDING, APPROVED, REJECTED.

-- =============================================================================
-- SMART BUSINESS LOGIC - PL/pgSQL TRIGGERS & FUNCTIONS
-- =============================================================================

-- 1. UTILITY FUNCTIONS (VALIDATION)
-- =============================================================================

-- Validate Date Ranges (Start < End) and Future Dates
CREATE OR REPLACE FUNCTION check_date_logic()
RETURNS TRIGGER AS $$
BEGIN
    -- Generic check for start_date and end_date columns if they exist
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
        IF NEW.start_date > NEW.end_date THEN
            RAISE EXCEPTION 'Start Date (%) cannot be after End Date (%)',
            NEW.start_date, NEW.end_date;
        END IF;

        -- Ensure End Date is in the future for NEW records (Business Rule)
        IF TG_OP = 'INSERT' AND NEW.end_date < NOW() THEN
             RAISE EXCEPTION 'End Date must be in the future for new records.';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. ORDER PROCESSING & INVENTORY (THE CORE)
-- =============================================================================

-- TRIGGER A: Inventory Check & Deduction + Product Active Check
CREATE OR REPLACE FUNCTION process_order_item_insert()
RETURNS TRIGGER AS $$
DECLARE
    v_stock_quantity INT;
    v_product_active BOOLEAN;
    v_product_cost NUMERIC(12,2);
BEGIN
    -- 1. Check Product Status (Must be Active) - User Request
    SELECT is_active, price INTO v_product_active, v_product_cost
    FROM product 
    WHERE id = NEW.product_id;

    IF v_product_active IS FALSE THEN
        RAISE EXCEPTION 'Cannot order inactive product (ID: %)', NEW.product_id;
    END IF;

    -- 2. Check Stock Availability
    SELECT SUM(quantity) INTO v_stock_quantity
    FROM inventory
    WHERE product_id = NEW.product_id;

    IF v_stock_quantity IS NULL OR v_stock_quantity < NEW.quantity THEN
        RAISE EXCEPTION 'Insufficient stock for Product ID %. Requested: %, Available: %', 
            NEW.product_id, NEW.quantity, COALESCE(v_stock_quantity, 0);
    END IF;

    -- 3. Auto-Calculate Item Amount
    IF NEW.unit_price IS NULL THEN
        SELECT price INTO NEW.unit_price FROM product WHERE id = NEW.product_id;
    END IF;
    
    NEW.amount := (NEW.unit_price * NEW.quantity) - COALESCE(NEW.discount_amount, 0);

    -- 4. Deduct Inventory (FIFO strategy)
    UPDATE inventory
    SET quantity = quantity - NEW.quantity
    WHERE id = (
        SELECT id FROM inventory 
        WHERE product_id = NEW.product_id AND quantity >= NEW.quantity
        ORDER BY quantity DESC 
        LIMIT 1
    );
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 
            'No single warehouse has enough stock for this item. Split shipment required (Not impl).';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER B: Update Order Totals & Profit
CREATE OR REPLACE FUNCTION update_order_totals()
RETURNS TRIGGER AS $$
DECLARE
    v_total_amount NUMERIC(12,2);
    v_total_cost NUMERIC(12,2);
BEGIN
    -- Calculate User Price
    SELECT COALESCE(SUM(amount), 0) INTO v_total_amount
    FROM order_items
    WHERE order_id = NEW.order_id;
    
    -- Calculate Approx Cost
    SELECT COALESCE(SUM(i.unit_cost * oi.quantity), 0)
    INTO v_total_cost
    FROM order_items oi
    JOIN inventory i ON i.product_id = oi.product_id
    WHERE oi.order_id = NEW.order_id
    LIMIT 1;

    -- Update Orders Table
    UPDATE orders
    SET total_price = v_total_amount,
        profit = v_total_amount - (v_total_amount * 0.7) -- Mock: 30% Margin
    WHERE id = NEW.order_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER C: Generate Unique Order Code
CREATE SEQUENCE IF NOT EXISTS order_code_seq;
CREATE OR REPLACE FUNCTION generate_order_code()
RETURNS TRIGGER AS $$
DECLARE
    v_date_part TEXT;
    v_seq_part TEXT;
BEGIN
    -- Lấy định dạng YYYYMMDD
    v_date_part := to_char(CURRENT_DATE, 'YYYYMMDD');
    
    -- Lấy giá trị sequence tiếp theo và format thành 5 chữ số (vd: 00001)
    v_seq_part := lpad(nextval('order_code_seq')::TEXT, 5, '0');
    
    -- Gán mã vào cột order_code
    NEW.order_code := 'ORD_' || v_date_part || '_' || v_seq_part;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- 3. ORDER STATE MACHINE
-- =============================================================================

CREATE OR REPLACE FUNCTION validate_order_status_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule: Delivered cannot go to Cancelled
    IF OLD.status = 'Delivered' AND NEW.status = 'Cancelled' THEN
        RAISE EXCEPTION 'Business Rule Violation: Cannot cancel a delivered order.';
    END IF;

    -- Logic: Log history
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO order_status_history (order_id, status, changed_at, changed_by)
        VALUES (NEW.id, NEW.status, NOW(), 'System_Trigger');
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 4. APPLY TRIGGERS
-- =============================================================================

DROP TRIGGER IF EXISTS trg_check_discount_dates ON discount;
CREATE TRIGGER trg_check_discount_dates
BEFORE INSERT OR UPDATE ON discount
FOR EACH ROW EXECUTE FUNCTION check_date_logic();

DROP TRIGGER IF EXISTS trg_order_item_insert ON order_items;
CREATE TRIGGER trg_order_item_insert
BEFORE INSERT ON order_items
FOR EACH ROW EXECUTE FUNCTION process_order_item_insert();
DROP TRIGGER IF EXISTS trg_generate_order_code ON orders;
CREATE TRIGGER trg_generate_order_code
BEFORE INSERT ON orders
FOR EACH ROW EXECUTE FUNCTION generate_order_code();

DROP TRIGGER IF EXISTS trg_order_item_after_change ON order_items;
CREATE TRIGGER trg_order_item_after_change
AFTER INSERT OR UPDATE OR DELETE ON order_items
FOR EACH ROW EXECUTE FUNCTION update_order_totals();

DROP TRIGGER IF EXISTS trg_order_status_check ON orders;
CREATE TRIGGER trg_order_status_check
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION validate_order_status_change();

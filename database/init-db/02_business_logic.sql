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

-- =============================================================================
-- v2.0: ENHANCED BUSINESS LOGIC FUNCTIONS
-- =============================================================================

-- 5. CUSTOMER TIER AUTO-UPDATE
-- =============================================================================
-- Tự động cập nhật hạng khách hàng dựa trên tổng chi tiêu
CREATE OR REPLACE FUNCTION update_customer_tier()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.total_spent IS DISTINCT FROM OLD.total_spent THEN
        IF NEW.total_spent >= 50000000 THEN
            NEW.tier := 'Platinum';
        ELSIF NEW.total_spent >= 20000000 THEN
            NEW.tier := 'Gold';
        ELSIF NEW.total_spent >= 5000000 THEN
            NEW.tier := 'Silver';
        ELSE
            NEW.tier := 'Bronze';
        END IF;

        -- Tích điểm loyalty dựa trên hạng
        NEW.loyalty_points := CASE NEW.tier
            WHEN 'Platinum' THEN FLOOR(NEW.total_spent / 10000) * 3
            WHEN 'Gold'     THEN FLOOR(NEW.total_spent / 10000) * 2
            WHEN 'Silver'   THEN FLOOR(NEW.total_spent / 10000) * 1.5
            ELSE                 FLOOR(NEW.total_spent / 10000)
        END;
    END IF;
    
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 6. VALIDATE PAYMENT AMOUNT (REFUND RULES)
-- =============================================================================
-- Đảm bảo hoàn tiền không vượt quá số tiền gốc
CREATE OR REPLACE FUNCTION validate_payment_amount()
RETURNS TRIGGER AS $$
BEGIN
    -- Khi chuyển sang Refunded, kiểm tra logic
    IF NEW.payment_status = 'Refunded' THEN
        IF OLD.payment_status != 'Completed' THEN
            RAISE EXCEPTION 'Chỉ có thể hoàn tiền cho giao dịch đã hoàn tất (Completed). Trạng thái hiện tại: %', 
                OLD.payment_status;
        END IF;
    END IF;
    
    -- Số tiền thanh toán phải > 0
    IF NEW.amount <= 0 THEN
        RAISE EXCEPTION 'Số tiền thanh toán phải lớn hơn 0. Giá trị: %', NEW.amount;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 7. VALIDATE RETURN REQUEST
-- =============================================================================
-- Chỉ cho phép trả hàng khi đơn hàng ở trạng thái Delivered hoặc Completed
CREATE OR REPLACE FUNCTION validate_return_request()
RETURNS TRIGGER AS $$
DECLARE
    v_order_status VARCHAR(50);
    v_order_total NUMERIC(12,2);
BEGIN
    -- Lấy trạng thái đơn hàng
    SELECT status, total_price INTO v_order_status, v_order_total
    FROM orders
    WHERE id = NEW.order_id;

    IF v_order_status IS NULL THEN
        RAISE EXCEPTION 'Đơn hàng ID % không tồn tại', NEW.order_id;
    END IF;

    -- Chỉ cho phép trả hàng khi đã giao hoặc hoàn tất
    IF v_order_status NOT IN ('Delivered', 'Completed') THEN
        RAISE EXCEPTION 'Không thể yêu cầu trả hàng cho đơn ở trạng thái "%". Chỉ chấp nhận Delivered hoặc Completed.', 
            v_order_status;
    END IF;

    -- Số tiền hoàn trả không vượt quá tổng đơn hàng
    IF NEW.refund_amount > v_order_total THEN
        RAISE EXCEPTION 'Số tiền hoàn trả (%) không được vượt quá tổng đơn hàng (%)', 
            NEW.refund_amount, v_order_total;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 8. LOG INVENTORY CHANGES (AUDIT TRAIL)
-- =============================================================================
-- Tự động ghi log khi số lượng tồn kho thay đổi
CREATE OR REPLACE FUNCTION log_inventory_change()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.quantity IS DISTINCT FROM OLD.quantity THEN
        INSERT INTO inventory_log (
            inventory_id, 
            change_type,
            quantity_change,
            quantity_before,
            quantity_after,
            changed_by
        ) VALUES (
            NEW.id,
            CASE 
                WHEN NEW.quantity > OLD.quantity THEN 'restock'
                WHEN NEW.quantity < OLD.quantity THEN 'sale'
                ELSE 'adjustment'
            END,
            NEW.quantity - OLD.quantity,
            OLD.quantity,
            NEW.quantity,
            'System_Trigger'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 9. SYNC SHIPMENT STATUS TO ORDER STATUS
-- =============================================================================
-- Đồng bộ trạng thái đơn hàng khi trạng thái vận chuyển thay đổi
CREATE OR REPLACE FUNCTION sync_shipment_to_order()
RETURNS TRIGGER AS $$
DECLARE
    v_order_id BIGINT;
BEGIN
    IF NEW.shipping_status IS DISTINCT FROM OLD.shipping_status THEN
        -- Tìm đơn hàng liên kết với vận đơn
        SELECT id INTO v_order_id
        FROM orders
        WHERE shipping_id = NEW.id;

        IF v_order_id IS NOT NULL THEN
            -- Đồng bộ trạng thái
            CASE NEW.shipping_status
                WHEN 'Delivered' THEN
                    UPDATE orders SET status = 'Delivered', delivered_at = NOW() 
                    WHERE id = v_order_id AND status NOT IN ('Cancelled', 'Refunded');
                WHEN 'InTransit' THEN
                    UPDATE orders SET status = 'Shipped' 
                    WHERE id = v_order_id AND status NOT IN ('Cancelled', 'Refunded', 'Delivered');
                WHEN 'ReturnToSender' THEN
                    UPDATE orders SET status = 'Returned' 
                    WHERE id = v_order_id AND status NOT IN ('Cancelled', 'Refunded');
                ELSE
                    NULL; -- Không đồng bộ các trạng thái khác
            END CASE;
        END IF;

        -- Cập nhật số lần giao hàng
        IF NEW.shipping_status = 'Failed' THEN
            NEW.delivery_attempts := COALESCE(OLD.delivery_attempts, 0) + 1;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 10. VALIDATE DISCOUNT VALUE
-- =============================================================================
-- Giá trị giảm giá % không vượt quá 70%
CREATE OR REPLACE FUNCTION validate_discount_value()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.discount_type = '%' AND NEW.value > 70 THEN
        RAISE EXCEPTION 'Giá trị giảm giá theo phần trăm không được vượt quá 70%%. Giá trị hiện tại: %', 
            NEW.value;
    END IF;

    -- Giá trị giảm giá phải dương
    IF NEW.value <= 0 THEN
        RAISE EXCEPTION 'Giá trị giảm giá phải lớn hơn 0. Giá trị: %', NEW.value;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 11. VALIDATE UNIQUE REVIEW
-- =============================================================================
-- Mỗi khách hàng chỉ đánh giá 1 lần cho 1 sản phẩm trên cùng 1 source
CREATE OR REPLACE FUNCTION validate_unique_review()
RETURNS TRIGGER AS $$
DECLARE
    v_existing INT;
BEGIN
    SELECT COUNT(*) INTO v_existing
    FROM product_review
    WHERE product_id = NEW.product_id
      AND customer_id = NEW.customer_id
      AND source_system = NEW.source_system
      AND is_active = TRUE;

    IF v_existing > 0 THEN
        RAISE EXCEPTION 'Khách hàng ID % đã đánh giá sản phẩm ID % trên nguồn "%" rồi.', 
            NEW.customer_id, NEW.product_id, NEW.source_system;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 4. APPLY TRIGGERS (ORIGINAL + v2.0)
-- =============================================================================

-- Original triggers
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

-- v2.0: New triggers
DROP TRIGGER IF EXISTS trg_customer_tier_update ON customers;
CREATE TRIGGER trg_customer_tier_update
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION update_customer_tier();

DROP TRIGGER IF EXISTS trg_validate_payment ON payment;
CREATE TRIGGER trg_validate_payment
BEFORE INSERT OR UPDATE ON payment
FOR EACH ROW EXECUTE FUNCTION validate_payment_amount();

DROP TRIGGER IF EXISTS trg_validate_return ON order_return;
CREATE TRIGGER trg_validate_return
BEFORE INSERT ON order_return
FOR EACH ROW EXECUTE FUNCTION validate_return_request();

DROP TRIGGER IF EXISTS trg_inventory_log ON inventory;
CREATE TRIGGER trg_inventory_log
AFTER UPDATE ON inventory
FOR EACH ROW EXECUTE FUNCTION log_inventory_change();

DROP TRIGGER IF EXISTS trg_shipment_sync ON shipment;
CREATE TRIGGER trg_shipment_sync
BEFORE UPDATE ON shipment
FOR EACH ROW EXECUTE FUNCTION sync_shipment_to_order();

DROP TRIGGER IF EXISTS trg_validate_discount_value ON discount;
CREATE TRIGGER trg_validate_discount_value
BEFORE INSERT OR UPDATE ON discount
FOR EACH ROW EXECUTE FUNCTION validate_discount_value();

DROP TRIGGER IF EXISTS trg_unique_review ON product_review;
CREATE TRIGGER trg_unique_review
BEFORE INSERT ON product_review
FOR EACH ROW EXECUTE FUNCTION validate_unique_review();

-- =============================================================================
-- v2.1: CART BUSINESS LOGIC
-- =============================================================================

-- 12. AUTO-CALCULATE CART ITEM AMOUNT
-- =============================================================================
-- Tự động tính amount = unit_price * quantity - discount_amount khi thêm/sửa item
CREATE OR REPLACE FUNCTION calc_cart_item_amount()
RETURNS TRIGGER AS $$
DECLARE
    v_product_price NUMERIC(12,2);
    v_product_active BOOLEAN;
BEGIN
    -- Kiểm tra sản phẩm còn active
    SELECT price, is_active INTO v_product_price, v_product_active
    FROM product WHERE id = NEW.product_id;

    IF v_product_active IS FALSE THEN
        RAISE EXCEPTION 'Không thể thêm sản phẩm đã ngừng bán (ID: %) vào giỏ hàng.', NEW.product_id;
    END IF;

    -- Snapshot giá nếu chưa có
    IF NEW.unit_price IS NULL OR NEW.unit_price = 0 THEN
        NEW.unit_price := v_product_price;
    END IF;

    -- Tự động tính amount
    NEW.amount := (NEW.unit_price * NEW.quantity) - COALESCE(NEW.discount_amount, 0);
    NEW.updated_at := NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 13. UPDATE CART TOTALS WHEN ITEMS CHANGE
-- =============================================================================
-- Cập nhật total_amount và item_count của giỏ hàng
CREATE OR REPLACE FUNCTION update_cart_totals()
RETURNS TRIGGER AS $$
DECLARE
    v_cart_id BIGINT;
    v_total NUMERIC(12,2);
    v_count INT;
BEGIN
    -- Xác định cart_id từ NEW hoặc OLD
    v_cart_id := COALESCE(NEW.cart_id, OLD.cart_id);

    -- Tính lại tổng
    SELECT COALESCE(SUM(amount), 0), COALESCE(COUNT(*), 0)
    INTO v_total, v_count
    FROM cart_items
    WHERE cart_id = v_cart_id AND is_active = TRUE;

    -- Cập nhật giỏ hàng
    UPDATE cart
    SET total_amount = v_total,
        item_count = v_count,
        updated_at = NOW()
    WHERE id = v_cart_id;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- 14. VALIDATE CART CHECKOUT
-- =============================================================================
-- Kiểm tra giỏ hàng trước khi checkout: phải active, phải có item, check tồn kho
CREATE OR REPLACE FUNCTION validate_cart_checkout()
RETURNS TRIGGER AS $$
DECLARE
    v_item RECORD;
    v_stock INT;
BEGIN
    -- Chỉ validate khi chuyển sang checked_out
    IF NEW.status = 'checked_out' AND OLD.status = 'active' THEN
        -- Giỏ phải có hàng
        IF NEW.item_count = 0 THEN
            RAISE EXCEPTION 'Không thể checkout giỏ hàng rỗng (Cart ID: %).', NEW.id;
        END IF;

        -- Kiểm tra tồn kho cho từng sản phẩm trong giỏ
        FOR v_item IN 
            SELECT ci.product_id, ci.quantity, p.name AS product_name
            FROM cart_items ci
            JOIN product p ON p.id = ci.product_id
            WHERE ci.cart_id = NEW.id AND ci.is_active = TRUE
        LOOP
            SELECT COALESCE(SUM(quantity), 0) INTO v_stock
            FROM inventory WHERE product_id = v_item.product_id;

            IF v_stock < v_item.quantity THEN
                RAISE EXCEPTION 'Sản phẩm "%" không đủ tồn kho. Yêu cầu: %, Còn lại: %',
                    v_item.product_name, v_item.quantity, v_stock;
            END IF;
        END LOOP;

        NEW.checked_out_at := NOW();
    END IF;

    -- Không cho phép thay đổi giỏ đã checkout
    IF OLD.status IN ('checked_out', 'expired') AND NEW.status = 'active' THEN
        RAISE EXCEPTION 'Không thể kích hoạt lại giỏ hàng ở trạng thái "%".', OLD.status;
    END IF;

    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- v2.1: Cart triggers
DROP TRIGGER IF EXISTS trg_calc_cart_item ON cart_items;
CREATE TRIGGER trg_calc_cart_item
BEFORE INSERT OR UPDATE ON cart_items
FOR EACH ROW EXECUTE FUNCTION calc_cart_item_amount();

DROP TRIGGER IF EXISTS trg_update_cart_totals ON cart_items;
CREATE TRIGGER trg_update_cart_totals
AFTER INSERT OR UPDATE OR DELETE ON cart_items
FOR EACH ROW EXECUTE FUNCTION update_cart_totals();

DROP TRIGGER IF EXISTS trg_cart_checkout ON cart;
CREATE TRIGGER trg_cart_checkout
BEFORE UPDATE ON cart
FOR EACH ROW EXECUTE FUNCTION validate_cart_checkout();

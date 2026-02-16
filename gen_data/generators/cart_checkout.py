from .base import BaseGenerator
from logger import logger

class CartCheckoutGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _cart_id BIGINT;
                _customer_id BIGINT;
                _customer_location BIGINT;
                _order_channel_id BIGINT;
                _channel VARCHAR(50);
                _cart_total NUMERIC(12,2);
                _payment_id BIGINT;
                _shipping_id BIGINT;
                _logistics_id BIGINT;
                _shipping_method_id BIGINT;
                _warehouse_id BIGINT;
                _estimated_hours INT;
                _payment_method VARCHAR(50);
                _transaction_id VARCHAR(100);
                _tracking_number VARCHAR(100);
                _shipping_cost NUMERIC(12,2);
                _is_expedited BOOLEAN;
                _new_order_id BIGINT;
                _item RECORD;
            BEGIN
                -- Tìm giỏ hàng active có items
                SELECT ct.id, ct.customer_id, ct.channel, ct.total_amount
                INTO _cart_id, _customer_id, _channel, _cart_total
                FROM cart ct
                WHERE ct.status = 'active' AND ct.is_active = TRUE 
                  AND ct.item_count > 0 AND ct.total_amount > 0
                ORDER BY RANDOM() LIMIT 1;

                IF _cart_id IS NULL THEN
                    RAISE NOTICE 'Không có giỏ hàng active có sản phẩm để checkout.';
                    RETURN;
                END IF;

                -- Lấy thông tin khách hàng
                SELECT geo_location_id, order_channel_id
                INTO _customer_location, _order_channel_id
                FROM customers WHERE id = _customer_id;

                -- Chọn logistics, shipping method, warehouse
                SELECT id INTO _logistics_id FROM logistics_partner WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1;
                SELECT id, estimated_delivery_time INTO _shipping_method_id, _estimated_hours FROM shipping_method WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1;
                SELECT id INTO _warehouse_id FROM warehouse WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1;

                -- Tạo Payment
                _payment_method := (ARRAY['MoMo', 'ZaloPay', 'VNPay', 'Credit Card', 'COD', 'Bank Transfer'])[
                    FLOOR(RANDOM() * 6 + 1)::INT];
                _transaction_id := 'TXN_' || TO_CHAR(NOW()::TIMESTAMP, 'YYYYMMDD') || '_' || LPAD(FLOOR(RANDOM() * 999999)::TEXT, 6, '0');

                INSERT INTO payment (payment_method, payment_status, transaction_id, amount, created_at, updated_at, is_active)
                VALUES (_payment_method, 'Completed', _transaction_id, _cart_total, NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE)
                RETURNING id INTO _payment_id;

                -- Tạo Shipment
                _tracking_number := 'TRK_' || TO_CHAR(NOW()::TIMESTAMP, 'YYYYMMDD') || '_' || LPAD(FLOOR(RANDOM() * 999999)::TEXT, 6, '0');
                _is_expedited := (RANDOM() < 0.3);
                _shipping_cost := CASE WHEN _is_expedited THEN FLOOR(RANDOM() * 50000 + 30000) ELSE FLOOR(RANDOM() * 30000 + 10000) END;

                INSERT INTO shipment (logistics_partner_id, warehouse_id, is_expedited, shipping_method_id,
                    tracking_number, shipping_cost, shipping_status, estimated_delivery, delivery_attempts,
                    created_at, updated_at, is_active)
                VALUES (_logistics_id, _warehouse_id, _is_expedited, _shipping_method_id,
                    _tracking_number, _shipping_cost, 'Created',
                    NOW()::TIMESTAMP + (_estimated_hours || ' hours')::INTERVAL, 0,
                    NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE)
                RETURNING id INTO _shipping_id;

                -- Tạo Order từ giỏ hàng
                INSERT INTO orders (
                    customer_id, payment_id, shipping_id, discount_id, location_id,
                    logistics_partner_id, order_channel_id,
                    order_date, status, created_at, updated_at,
                    total_price, profit, notes
                ) VALUES (
                    _customer_id, _payment_id, _shipping_id, NULL, _customer_location,
                    _logistics_id, _order_channel_id,
                    NOW()::TIMESTAMP, 'Pending', NOW()::TIMESTAMP, NOW()::TIMESTAMP,
                    _cart_total, ROUND((_cart_total * (RANDOM() * 0.2 + 0.15))::NUMERIC, 2),
                    'Checkout từ giỏ hàng #' || _cart_id
                )
                RETURNING id INTO _new_order_id;

                -- Copy cart_items thành order_items
                FOR _item IN
                    SELECT product_id, quantity, unit_price, discount_amount, amount
                    FROM cart_items WHERE cart_id = _cart_id AND is_active = TRUE
                LOOP
                    INSERT INTO order_items (order_id, product_id, unit_price, discount_amount, quantity, amount, is_active, created_at)
                    VALUES (_new_order_id, _item.product_id, _item.unit_price, _item.discount_amount, _item.quantity, _item.amount, TRUE, NOW()::TIMESTAMP);
                END LOOP;

                -- Checkout giỏ hàng (trigger trg_cart_checkout validates)
                UPDATE cart SET status = 'checked_out', order_id = _new_order_id WHERE id = _cart_id;

                -- Cập nhật khách hàng
                UPDATE customers SET last_order_date = NOW()::TIMESTAMP WHERE id = _customer_id;

                RAISE NOTICE 'Checkout giỏ ID: % → Order ID: %, Total: %', _cart_id, _new_order_id, _cart_total;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Cart Checkout thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong CartCheckoutGenerator: {e}")
            return False

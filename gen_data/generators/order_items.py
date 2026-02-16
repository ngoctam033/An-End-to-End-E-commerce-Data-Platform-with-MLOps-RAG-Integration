from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class OrderItemsGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _target_order_id BIGINT;
                _order_created_at TIMESTAMP;
                _order_discount_id BIGINT;
                _discount_type VARCHAR(50);
                _discount_value NUMERIC(12,2);
                _product_id BIGINT;
                _unit_price NUMERIC(12, 2);
                _quantity INT;
                _discount_amount NUMERIC(12, 2);
                _item_amount NUMERIC(12, 2);
                _total_order_price NUMERIC(12, 2) := 0;
                _num_items INT;
                i INT;
            BEGIN
                -- ==================================================================
                -- BƯỚC 1: TÌM ĐƠN HÀNG PENDING CHƯA CÓ ITEM
                -- ==================================================================
                SELECT o.id, o.order_date, o.discount_id
                INTO _target_order_id, _order_created_at, _order_discount_id
                FROM orders o
                WHERE NOT EXISTS (
                    SELECT 1 FROM order_items oi WHERE oi.order_id = o.id
                )
                AND o.status = 'Pending'
                ORDER BY RANDOM() LIMIT 1;

                IF _target_order_id IS NULL THEN
                    RAISE NOTICE 'Không tìm thấy đơn hàng Pending chưa có items.';
                    RETURN;
                END IF;

                -- Lấy thông tin discount nếu có
                IF _order_discount_id IS NOT NULL THEN
                    SELECT discount_type, value INTO _discount_type, _discount_value
                    FROM discount WHERE id = _order_discount_id;
                END IF;

                -- ==================================================================
                -- BƯỚC 2: TẠO CÁC ORDER ITEMS
                -- ==================================================================
                _num_items := FLOOR(RANDOM() * 4 + 1)::INT; -- 1-4 loại sản phẩm

                FOR i IN 1.._num_items LOOP
                    -- Chọn sản phẩm active ngẫu nhiên
                    SELECT id, price INTO _product_id, _unit_price
                    FROM product WHERE is_active = TRUE
                    ORDER BY RANDOM() LIMIT 1;

                    -- Random số lượng 1-3
                    _quantity := FLOOR(RANDOM() * 3 + 1)::INT;

                    -- Tính discount
                    _discount_amount := 0;
                    IF _discount_type = '%' THEN
                        _discount_amount := ROUND((_unit_price * _quantity * _discount_value / 100)::NUMERIC, 2);
                    ELSIF _discount_type = 'fixed' THEN
                        -- Chia đều fixed discount cho mỗi item
                        _discount_amount := ROUND((_discount_value / _num_items)::NUMERIC, 2);
                    END IF;

                    _item_amount := (_unit_price * _quantity) - _discount_amount;
                    IF _item_amount < 0 THEN _item_amount := 0; END IF;

                    _total_order_price := _total_order_price + _item_amount;

                    INSERT INTO order_items (
                        order_id, product_id, unit_price, discount_amount,
                        quantity, amount, is_active, created_at
                    ) VALUES (
                        _target_order_id, _product_id, _unit_price, _discount_amount,
                        _quantity, _item_amount, TRUE, _order_created_at
                    );
                END LOOP;

                -- Guard: đảm bảo tổng đơn hàng > 0 (tránh trigger validate_payment_amount)
                IF _total_order_price <= 0 THEN
                    _total_order_price := 1;
                END IF;

                -- ==================================================================
                -- BƯỚC 3: CẬP NHẬT ORDER HEADER
                -- ==================================================================
                UPDATE orders
                SET total_price = _total_order_price,
                    profit = ROUND((_total_order_price * (RANDOM() * 0.2 + 0.15))::NUMERIC, 2), -- 15-35% profit
                    updated_at = NOW()::TIMESTAMP
                WHERE id = _target_order_id;

                -- Cập nhật payment amount
                UPDATE payment
                SET amount = _total_order_price, updated_at = NOW()::TIMESTAMP
                WHERE id = (SELECT payment_id FROM orders WHERE id = _target_order_id);

                RAISE NOTICE 'Order ID: % - Items: %, Total: %', _target_order_id, _num_items, _total_order_price;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Order Items thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong OrderItemsGenerator: {e}")
            return False
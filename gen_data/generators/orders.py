from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class OrderGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _new_order_id BIGINT;
                _customer_id BIGINT;
                _customer_location BIGINT;
                _order_channel_id BIGINT;
                _payment_id BIGINT;
                _shipping_id BIGINT;
                _logistics_id BIGINT;
                _shipping_method_id BIGINT;
                _warehouse_id BIGINT;
                _discount_id BIGINT;
                _payment_method VARCHAR(50);
                _transaction_id VARCHAR(100);
                _tracking_number VARCHAR(100);
                _shipping_cost NUMERIC(12,2);
                _is_expedited BOOLEAN;
                _estimated_hours INT;
            BEGIN
                -- ==================================================================
                -- BƯỚC 1: LẤY DỮ LIỆU NGẪU NHIÊN
                -- ==================================================================

                -- Chọn khách hàng active ngẫu nhiên
                SELECT id, geo_location_id, order_channel_id
                INTO _customer_id, _customer_location, _order_channel_id
                FROM customers WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                IF _customer_id IS NULL THEN
                    RAISE NOTICE 'Không có khách hàng nào. Bỏ qua.';
                    RETURN;
                END IF;

                -- Chọn logistics partner ngẫu nhiên
                SELECT id INTO _logistics_id
                FROM logistics_partner WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                -- Chọn shipping method ngẫu nhiên
                SELECT id, estimated_delivery_time INTO _shipping_method_id, _estimated_hours
                FROM shipping_method WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                -- Chọn warehouse ngẫu nhiên
                SELECT id INTO _warehouse_id
                FROM warehouse WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                -- ==================================================================
                -- BƯỚC 2: TẠO PAYMENT RECORD
                -- ==================================================================
                _payment_method := (ARRAY['MoMo', 'ZaloPay', 'VNPay', 'Credit Card', 'COD', 'Bank Transfer'])[
                    FLOOR(RANDOM() * 6 + 1)::INT
                ];
                _transaction_id := 'TXN_' || TO_CHAR(NOW()::TIMESTAMP, 'YYYYMMDD') || '_' || LPAD(FLOOR(RANDOM() * 999999)::TEXT, 6, '0');

                INSERT INTO payment (payment_method, payment_status, transaction_id, amount, created_at, updated_at, is_active)
                VALUES (_payment_method, 'Pending', _transaction_id, 1, NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE)
                RETURNING id INTO _payment_id;

                -- ==================================================================
                -- BƯỚC 3: TẠO SHIPMENT RECORD
                -- ==================================================================
                _tracking_number := 'TRK_' || TO_CHAR(NOW()::TIMESTAMP, 'YYYYMMDD') || '_' || LPAD(FLOOR(RANDOM() * 999999)::TEXT, 6, '0');
                _is_expedited := (RANDOM() < 0.3);
                _shipping_cost := CASE
                    WHEN _is_expedited THEN FLOOR(RANDOM() * 50000 + 30000)
                    ELSE FLOOR(RANDOM() * 30000 + 10000)
                END;

                INSERT INTO shipment (
                    logistics_partner_id, warehouse_id, is_expedited, shipping_method_id,
                    tracking_number, shipping_cost, shipping_status,
                    estimated_delivery, delivery_attempts,
                    created_at, updated_at, is_active
                ) VALUES (
                    _logistics_id, _warehouse_id, _is_expedited, _shipping_method_id,
                    _tracking_number, _shipping_cost, 'Created',
                    NOW()::TIMESTAMP + (_estimated_hours || ' hours')::INTERVAL, 0,
                    NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE
                )
                RETURNING id INTO _shipping_id;

                -- ==================================================================
                -- BƯỚC 4: CHỌN DISCOUNT (30% cơ hội)
                -- ==================================================================
                IF RANDOM() < 0.3 THEN
                    SELECT id INTO _discount_id
                    FROM discount
                    WHERE is_active = TRUE
                      AND start_date <= NOW()::TIMESTAMP
                      AND end_date >= NOW()::TIMESTAMP
                    ORDER BY RANDOM() LIMIT 1;
                END IF;

                -- ==================================================================
                -- BƯỚC 5: TẠO ORDER (status = 'Pending', trigger tạo order_code)
                -- ==================================================================
                INSERT INTO orders (
                    customer_id, payment_id, shipping_id, discount_id, location_id,
                    logistics_partner_id, order_channel_id,
                    order_date, status, created_at, updated_at,
                    total_price, profit
                ) VALUES (
                    _customer_id, _payment_id, _shipping_id, _discount_id, _customer_location,
                    _logistics_id, _order_channel_id,
                    NOW()::TIMESTAMP, 'Pending', NOW()::TIMESTAMP, NOW()::TIMESTAMP,
                    0, 0
                )
                RETURNING id INTO _new_order_id;

                -- Cập nhật last_order_date cho khách hàng
                UPDATE customers SET last_order_date = NOW()::TIMESTAMP WHERE id = _customer_id;

                RAISE NOTICE 'Đã tạo Order ID: % cho Customer ID: %', _new_order_id, _customer_id;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Order thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong OrderGenerator: {e}")
            return False
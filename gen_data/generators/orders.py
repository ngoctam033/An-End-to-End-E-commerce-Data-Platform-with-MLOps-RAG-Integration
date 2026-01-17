from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class OrderGenerator(BaseGenerator):
    def __init__(self):
        # Khởi tạo class cha
        super().__init__()
        self.sql_command = """
            -- Tạo đơn hàng với tổng tiền = 0, chưa có sản phẩm
            DO $$
            DECLARE
                _new_order_id BIGINT;
                _customer_id BIGINT;
                _order_channel_id BIGINT;
                _random_payment_id BIGINT;
                _random_shipping_id BIGINT;
                _random_logistics_id BIGINT;
                _random_location_id BIGINT;
                _order_code VARCHAR(50);
                _order_date TIMESTAMP;
            BEGIN
                -- ==================================================================
                -- BƯỚC 1: LẤY DỮ LIỆU NGẪU NHIÊN
                -- ==================================================================
                
                -- Lấy ngẫu nhiên 1 khách hàng
                SELECT id, geo_location_id, order_channel_id
                INTO _customer_id, _random_location_id, _order_channel_id
                FROM customers
                ORDER BY RANDOM()
                LIMIT 1;

                -- Lấy ngẫu nhiên thông tin thanh toán, vận chuyển
                SELECT id INTO _random_payment_id FROM payment ORDER BY RANDOM() LIMIT 1;
                SELECT id INTO _random_shipping_id FROM shipment ORDER BY RANDOM() LIMIT 1;
                SELECT id INTO _random_logistics_id FROM logistics_partner ORDER BY RANDOM() LIMIT 1;

                _order_date := NOW();
                -- Sinh mã đơn hàng: ORD + YYYYMMDD + Random 4 số
                -- _order_code := 'ORD' || TO_CHAR(_order_date, 'YYYYMMDD') || TRUNC(RANDOM() * 9000 + 1000)::TEXT;

                -- ==================================================================
                -- BƯỚC 2: INSERT ORDER HEADER
                -- ==================================================================
                INSERT INTO orders (
                    customer_id, payment_id, shipping_id, discount_id, location_id,
                    logistics_partner_id, order_channel_id,
                    -- order_code,
                    order_date, status, created_at, updated_at,
                    total_price, profit
                )
                VALUES (
                    _customer_id, _random_payment_id, _random_shipping_id, NULL, _random_location_id,
                    _random_logistics_id, _order_channel_id,
                    -- _order_code,
                    _order_date, 'completed', NOW(), NOW(),
                    0, 0 -- Giá trị khởi tạo là 0
                )
                RETURNING id INTO _new_order_id;

                RAISE NOTICE 'Đã tạo Order Header thành công. ID: %, Code: %', _new_order_id, _order_code;

            END $$;
            """

    def generate(self, params=None):
        """
        Tạo dữ liệu giả và lưu vào bảng orders bằng 100% câu lệnh SQL.
        """
        result = False
        try:
            # Thực thi thông qua class cha
            result = super().generate()
            
            if result == True:
                logger.info("Generated Order thành công bằng 100% SQL logic.")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi trong OrderGenerator: {e}")
            return False
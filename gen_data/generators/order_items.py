from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class OrderItemsGenerator(BaseGenerator):
    def __init__(self):
        # Khởi tạo class cha
        super().__init__()
        self.sql_command = """
            -- SCRIPT: TẠO ITEMS CHO ĐƠN HÀNG TRỐNG
            -- Tìm đơn hàng chưa có item để thêm sản phẩm và cập nhật doanh thu
            DO $$
            DECLARE
                _target_order_id BIGINT;
                _product_id BIGINT;
                _unit_price NUMERIC(12, 2);
                _quantity INT;
                _item_amount NUMERIC(12, 2);
                _total_order_price NUMERIC(12, 2) := 0;
                _num_items INT;
                i INT;
            BEGIN
                -- ==================================================================
                -- BƯỚC 1: TÌM ĐƠN HÀNG CHƯA CÓ ITEM
                -- ==================================================================
                
                -- Logic: Chọn ID từ bảng orders mà ID đó chưa xuất hiện trong bảng order_items
                SELECT id INTO _target_order_id
                FROM orders o
                WHERE NOT EXISTS (
                    SELECT 1 FROM order_items oi WHERE oi.order_id = o.id
                )
                ORDER BY RANDOM()
                LIMIT 1;

                -- Kiểm tra nếu không tìm thấy đơn hàng nào
                IF _target_order_id IS NULL THEN
                    RAISE NOTICE 'Không tìm thấy đơn hàng nào chưa có items (sản phẩm).';
                    RETURN;
                END IF;

                RAISE NOTICE 'Đang xử lý tạo items cho Order ID: %', _target_order_id;

                -- ==================================================================
                -- BƯỚC 2: TẠO CÁC ORDER ITEMS
                -- ==================================================================

                -- Random số lượng loại sản phẩm (SKU) sẽ mua (1-5 loại)
                _num_items := FLOOR(RANDOM() * 5 + 1)::INT;

                FOR i IN 1.._num_items LOOP
                    -- Lấy ngẫu nhiên sản phẩm
                    SELECT id, price INTO _product_id, _unit_price
                    FROM product
                    ORDER BY RANDOM()
                    LIMIT 1;

                    -- Random số lượng mua (quantity) cho mỗi loại
                    _quantity := FLOOR(RANDOM() * 10 + 1)::INT;

                    -- Tính thành tiền
                    _item_amount := _unit_price * _quantity;

                    -- Cộng dồn tổng tiền đơn hàng
                    _total_order_price := _total_order_price + _item_amount;

                    -- Insert vào bảng order_items
                    INSERT INTO order_items (
                        order_id, product_id, unit_price, discount_amount,
                        quantity, amount, is_active
                    )
                    VALUES (
                        _target_order_id, _product_id, _unit_price, 0,
                        _quantity, _item_amount, true
                    );
                END LOOP;

                -- ==================================================================
                -- BƯỚC 3: CẬP NHẬT LẠI HEADER (TOTAL PRICE)
                -- ==================================================================
                UPDATE orders
                SET
                    total_price = _total_order_price,
                    profit = _total_order_price * 0.3, -- Giả định profit 30%
                    updated_at = NOW()
                WHERE id = _target_order_id;

                RAISE NOTICE 'Đã hoàn tất cập nhật Order ID: %. Tổng tiền: %', _target_order_id, _total_order_price;

            END $$;
                """
    def generate(self, params=None):
        """
        Tạo dữ liệu giả và lưu vào bảng order_items bằng 100% câu lệnh SQL.
        """
        result = False
        try:
            # Thực thi thông qua class cha
            result = super().generate()
            
            if result == True:
                logger.info("Generated Order Items thành công bằng 100% SQL logic.")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi trong OrderItemsGenerator: {e}")
            return False
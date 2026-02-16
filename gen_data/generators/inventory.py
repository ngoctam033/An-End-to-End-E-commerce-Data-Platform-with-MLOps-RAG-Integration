from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class InventoryGenerator(BaseGenerator):
    def __init__(self):
        # Khởi tạo class cha
        super().__init__()
        self.sql_command = """
            -- ==================================================================
            -- LOGIC CẬP NHẬT TỒN KHO NGẪU NHIÊN CHO 1 CẶP SẢN PHẨM - KHO HÀNG
            -- ==================================================================
            -- Script này sẽ chọn ngẫu nhiên 1 kho và 1 sản phẩm trong kho đó
            -- Sau đó tăng số lượng tồn kho lên một khoảng ngẫu nhiên (10-20)
            DO $$
            DECLARE
                _selected_warehouse_id INT;
                _selected_product_id BIGINT;
                _current_qty INT;
                _add_qty INT;
                _new_qty INT;
                _warehouse_name VARCHAR(100);
            BEGIN
                -- 1. Chọn random một kho hàng (chỉ chọn kho nào ĐÃ có tồn kho)
                SELECT w.id, w.name 
                INTO _selected_warehouse_id, _warehouse_name
                FROM warehouse w
                JOIN inventory i ON w.id = i.warehouse_id
                GROUP BY w.id, w.name
                ORDER BY random()
                LIMIT 1;

                IF _selected_warehouse_id IS NULL THEN
                    RAISE NOTICE '⚠️ Không tìm thấy kho hàng nào có tồn kho để cập nhật!';
                    RETURN;
                END IF;

                -- 2. Chọn random một sản phẩm TRONG KHO ĐÓ
                SELECT product_id, quantity
                INTO _selected_product_id, _current_qty
                FROM inventory
                WHERE warehouse_id = _selected_warehouse_id
                ORDER BY random()
                LIMIT 1;

                -- 3. Tính lượng tăng thêm (random 10 - 20)
                -- Công thức: floor(random() * (max - min + 1) + min)
                _add_qty := floor(random() * (20 - 10 + 1) + 10)::int;
                _new_qty := _current_qty + _add_qty;

                -- 4. Cập nhật Database
                UPDATE inventory
                SET quantity = _new_qty,
                    updated_at = NOW()::TIMESTAMP
                WHERE warehouse_id = _selected_warehouse_id 
                AND product_id = _selected_product_id;

                -- 5. Log kết quả
                RAISE NOTICE '------------------------------------------------';
                RAISE NOTICE '🏭 Kho được chọn: % (ID: %)', _warehouse_name, _selected_warehouse_id;
                RAISE NOTICE '📦 Sản phẩm ID: %', _selected_product_id;
                RAISE NOTICE '📊 Tồn kho cũ: %', _current_qty;
                RAISE NOTICE '➕ Nhập thêm: %', _add_qty;
                RAISE NOTICE '✅ Tồn kho mới: %', _new_qty;
                RAISE NOTICE '------------------------------------------------';

            END $$;
            """
    def generate(self, params=None):
        """
        Tạo dữ liệu giả và lưu vào bảng inventory bằng 100% câu lệnh SQL.
        """
        result = False
        try:
            # Thực thi thông qua class cha
            result = super().generate()
            
            if result == True:
                logger.info("Generated Inventory thành công bằng 100% SQL logic.")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi trong InventoryGenerator: {e}")
            return False
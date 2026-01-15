from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class CustomersGenerator(BaseGenerator):
    def generate(self, params=None):
        """
        Tạo dữ liệu giả và lưu vào bảng customers bằng 100% câu lệnh SQL.
        """
        result = False
        try:
            self.sql_command = """
                -- Sử dụng khối lệnh DO (PL/pgSQL) để xử lý logic sinh mã phức tạp
                DO $$
                DECLARE
                    _random_ward_code BIGINT;
                    _random_channel_id BIGINT;
                    _channel_code VARCHAR(20);      -- Biến lưu mã viết tắt của kênh (ví dụ: 'FB', 'WEB')
                    _current_count BIGINT;          -- Biến đếm số lượng khách hàng hiện tại
                    _new_customer_code VARCHAR(50); -- Biến lưu mã khách hàng cuối cùng
                BEGIN
                    -- Bước 1: Tìm ward_code ngẫu nhiên
                    SELECT ward_code INTO _random_ward_code
                    FROM geo_location
                    ORDER BY RANDOM()
                    LIMIT 1;

                    -- Bước 2: Tìm channel_id VÀ channel_code ngẫu nhiên
                    -- Lưu ý: Giả định bảng order_channel có cột 'channel_code' (hoặc bạn đổi thành tên cột tương ứng chứa mã viết tắt)
                    SELECT id, name INTO _random_channel_id, _channel_code
                    FROM order_channel
                    ORDER BY RANDOM()
                    LIMIT 1;

                    -- Bước 3: Tính số thứ tự của khách hàng trong kênh này
                    -- Đếm số khách hàng hiện có thuộc kênh vừa lấy được
                    SELECT COUNT(*) INTO _current_count
                    FROM customers
                    WHERE order_channel_id = _random_channel_id;

                    -- Bước 4: Tạo mã khách hàng theo format: KH + [Mã Kênh] + [Số thứ tự 3 chữ số]
                    -- Ví dụ: Kênh là 'FB', đang có 5 khách -> KH + FB + 006 => KHFB006
                    _new_customer_code := 'KH' || '_' || _channel_code || '_' || LPAD((_current_count + 1)::TEXT, 3, '0');

                    -- Bước 5: Thực hiện Insert
                    INSERT INTO customers (
                        customer_code,
                        geo_location_id,
                        order_channel_id,
                        created_at
                    )
                    VALUES (
                        _new_customer_code,
                        _random_ward_code,
                        _random_channel_id,
                        NOW()
                    );

                    -- (Tuỳ chọn) In ra mã vừa tạo để kiểm tra trong phần Messages của tool quản lý DB
                    RAISE NOTICE 'Đã tạo khách hàng mới với mã: %', _new_customer_code;
                END $$;
                """
            
            # Vì mọi thứ đã được xử lý trong SQL, sql_params sẽ là None
            self.sql_params = None
            
            # Thực thi thông qua class cha
            result = super().generate()
            
            if result == True:
                logger.info("Generated customer thành công bằng 100% SQL logic.")
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi trong CustomersGenerator: {e}")
            return False
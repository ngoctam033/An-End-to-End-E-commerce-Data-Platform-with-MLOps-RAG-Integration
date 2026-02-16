from .base import BaseGenerator
from logger import logger
import random
from datetime import datetime

class CustomersGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _random_ward_code BIGINT;
                _random_channel_id BIGINT;
                _channel_code VARCHAR(20);
                _current_count BIGINT;
                _new_customer_code VARCHAR(50);
                _name VARCHAR(100);
                _email VARCHAR(100);
                _phone VARCHAR(20);
            BEGIN
                -- Bước 1: Chọn vị trí ngẫu nhiên
                SELECT ward_code INTO _random_ward_code
                FROM geo_location ORDER BY RANDOM() LIMIT 1;

                -- Bước 2: Chọn kênh ngẫu nhiên
                SELECT id, name INTO _random_channel_id, _channel_code
                FROM order_channel ORDER BY RANDOM() LIMIT 1;

                -- Bước 3: Đếm khách hàng trong kênh
                SELECT COUNT(*) INTO _current_count
                FROM customers WHERE order_channel_id = _random_channel_id;

                -- Bước 4: Tạo mã khách hàng
                _new_customer_code := 'KH_' || _channel_code || '_' || LPAD((_current_count + 1)::TEXT, 3, '0');

                -- Bước 5: Sinh thông tin cá nhân
                _name := (ARRAY[
                    'Nguyễn Văn', 'Trần Thị', 'Lê Minh', 'Phạm Hoàng', 'Hoàng Thu',
                    'Vũ Đức', 'Đặng Ngọc', 'Bùi Quang', 'Đỗ Thanh', 'Ngô Phương',
                    'Dương Hải', 'Lý Thành', 'Phan Anh', 'Hồ Bích', 'Tô Minh'
                ])[FLOOR(RANDOM() * 15 + 1)::INT]
                || ' ' || (ARRAY[
                    'An', 'Bình', 'Châu', 'Dũng', 'Em', 'Giang', 'Hà', 'Khoa',
                    'Lan', 'Minh', 'Nam', 'Phúc', 'Quân', 'Sơn', 'Tú', 'Uyên', 'Vy'
                ])[FLOOR(RANDOM() * 17 + 1)::INT];

                _email := LOWER(REPLACE(_new_customer_code, '_', '.')) || '@gmail.com';
                _phone := '09' || LPAD(FLOOR(RANDOM() * 100000000)::TEXT, 8, '0');

                -- Bước 6: Insert
                INSERT INTO customers (
                    customer_code, geo_location_id, order_channel_id,
                    name, email, phone,
                    tier, total_spent, loyalty_points,
                    created_at, updated_at, is_active
                ) VALUES (
                    _new_customer_code, _random_ward_code, _random_channel_id,
                    _name, _email, _phone,
                    'Bronze', 0, 0,
                    NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE
                );

                RAISE NOTICE 'Đã tạo khách hàng: % - %', _new_customer_code, _name;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated customer thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong CustomersGenerator: {e}")
            return False
from .base import BaseGenerator
from logger import logger

class ActivityLogGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _customer_id BIGINT;
                _product_id BIGINT;
                _activity VARCHAR(50);
                _channel VARCHAR(50);
                _metadata JSONB;
                _num_activities INT;
                i INT;
            BEGIN
                -- Chọn khách hàng ngẫu nhiên
                SELECT id INTO _customer_id
                FROM customers WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                IF _customer_id IS NULL THEN RETURN; END IF;

                _num_activities := FLOOR(RANDOM() * 4 + 1)::INT; -- 1-4 hoạt động

                FOR i IN 1.._num_activities LOOP
                    -- Loại hoạt động với xác suất thực tế
                    _activity := (ARRAY[
                        'view', 'view', 'view', 'view',    -- 40%
                        'search', 'search',                 -- 20%
                        'add_to_cart', 'add_to_cart',       -- 20%
                        'wishlist',                         -- 10%
                        'purchase'                          -- 10%
                    ])[FLOOR(RANDOM() * 10 + 1)::INT];

                    _channel := (ARRAY['Shopee', 'Lazada', 'Tiki', 'Website'])[
                        FLOOR(RANDOM() * 4 + 1)::INT];

                    -- Chọn sản phẩm ngẫu nhiên (cho view, add_to_cart, wishlist)
                    IF _activity IN ('view', 'add_to_cart', 'wishlist', 'purchase') THEN
                        SELECT id INTO _product_id
                        FROM product WHERE is_active = TRUE
                        ORDER BY RANDOM() LIMIT 1;
                    ELSE
                        _product_id := NULL;
                    END IF;

                    -- Metadata
                    _metadata := CASE _activity
                        WHEN 'search' THEN jsonb_build_object(
                            'keyword', (ARRAY['iPhone', 'Samsung', 'laptop', 'tai nghe', 'sạc dự phòng',
                                             'ốp lưng', 'chuột gaming', 'bàn phím', 'màn hình'])[
                                FLOOR(RANDOM() * 9 + 1)::INT],
                            'results_count', FLOOR(RANDOM() * 50 + 1)::INT
                        )
                        WHEN 'view' THEN jsonb_build_object(
                            'duration_seconds', FLOOR(RANDOM() * 300 + 5)::INT,
                            'source', (ARRAY['direct', 'search', 'recommendation', 'ad'])[
                                FLOOR(RANDOM() * 4 + 1)::INT]
                        )
                        WHEN 'add_to_cart' THEN jsonb_build_object(
                            'quantity', FLOOR(RANDOM() * 3 + 1)::INT
                        )
                        ELSE '{}'::JSONB
                    END;

                    INSERT INTO customer_activity_log (customer_id, activity_type, product_id, channel, metadata, created_at)
                    VALUES (_customer_id, _activity, _product_id, _channel, _metadata, NOW()::TIMESTAMP);
                END LOOP;

                RAISE NOTICE 'Logged % hoạt động cho KH %', _num_activities, _customer_id;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Activity Log thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong ActivityLogGenerator: {e}")
            return False

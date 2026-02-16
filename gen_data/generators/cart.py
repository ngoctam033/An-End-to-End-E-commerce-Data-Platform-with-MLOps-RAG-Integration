from .base import BaseGenerator
from logger import logger

class CartGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _customer_id BIGINT;
                _channel VARCHAR(50);
                _cart_id BIGINT;
            BEGIN
                -- Chọn khách hàng active chưa có giỏ active trên kênh này
                SELECT c.id INTO _customer_id
                FROM customers c
                WHERE c.is_active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM cart ct
                      WHERE ct.customer_id = c.id AND ct.status = 'active'
                  )
                ORDER BY RANDOM() LIMIT 1;

                IF _customer_id IS NULL THEN
                    -- Nếu tất cả KH đều có giỏ active, chọn bất kỳ KH nào
                    SELECT id INTO _customer_id FROM customers
                    WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1;
                END IF;

                IF _customer_id IS NULL THEN
                    RAISE NOTICE 'Không có khách hàng.';
                    RETURN;
                END IF;

                _channel := (ARRAY['Shopee', 'Lazada', 'Tiki', 'Website'])[
                    FLOOR(RANDOM() * 4 + 1)::INT
                ];

                INSERT INTO cart (customer_id, channel, status, total_amount, item_count,
                                  expires_at, is_active, created_at, updated_at)
                VALUES (_customer_id, _channel, 'active', 0, 0,
                        NOW()::TIMESTAMP + INTERVAL '7 days', TRUE, NOW()::TIMESTAMP, NOW()::TIMESTAMP)
                RETURNING id INTO _cart_id;

                RAISE NOTICE 'Tạo giỏ hàng ID: % cho KH ID: % trên %', _cart_id, _customer_id, _channel;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Cart thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong CartGenerator: {e}")
            return False

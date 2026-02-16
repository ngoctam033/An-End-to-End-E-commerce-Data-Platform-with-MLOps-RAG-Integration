from .base import BaseGenerator
from logger import logger

class WishlistGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _customer_id BIGINT;
                _product_id BIGINT;
            BEGIN
                -- Chọn khách hàng ngẫu nhiên
                SELECT id INTO _customer_id
                FROM customers WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                -- Chọn sản phẩm active chưa có trong wishlist
                SELECT p.id INTO _product_id
                FROM product p
                WHERE p.is_active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM wishlist w
                      WHERE w.customer_id = _customer_id AND w.product_id = p.id
                  )
                ORDER BY RANDOM() LIMIT 1;

                IF _product_id IS NULL THEN
                    RAISE NOTICE 'KH % đã thêm tất cả sản phẩm vào wishlist.', _customer_id;
                    RETURN;
                END IF;

                INSERT INTO wishlist (customer_id, product_id, added_at, is_active)
                VALUES (_customer_id, _product_id, NOW()::TIMESTAMP, TRUE);

                RAISE NOTICE 'Wishlist: KH % → SP %', _customer_id, _product_id;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Wishlist thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong WishlistGenerator: {e}")
            return False

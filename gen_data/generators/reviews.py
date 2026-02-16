from .base import BaseGenerator
from logger import logger

class ReviewGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _customer_id BIGINT;
                _product_id BIGINT;
                _rating INT;
                _review_text TEXT;
                _source VARCHAR(50);
            BEGIN
                -- Tìm khách hàng có đơn Completed, chưa review sản phẩm đó
                WITH distinct_pairs AS (
                    SELECT DISTINCT o.customer_id, oi.product_id
                    FROM orders o
                    JOIN order_items oi ON oi.order_id = o.id
                    WHERE o.status = 'Completed'
                      AND NOT EXISTS (
                          SELECT 1 FROM product_review pr
                          WHERE pr.customer_id = o.customer_id
                            AND pr.product_id = oi.product_id
                      )
                )
                SELECT customer_id, product_id
                INTO _customer_id, _product_id
                FROM distinct_pairs
                ORDER BY RANDOM() LIMIT 1;

                IF _customer_id IS NULL THEN
                    RAISE NOTICE 'Không có sản phẩm nào cần review.';
                    RETURN;
                END IF;

                _rating := FLOOR(RANDOM() * 5 + 1)::INT;

                _review_text := CASE _rating
                    WHEN 5 THEN (ARRAY[
                        'Sản phẩm tuyệt vời! Rất hài lòng.',
                        'Chất lượng xuất sắc, giao hàng nhanh.',
                        'Rất đáng tiền, sẽ mua lại.',
                        'Tốt hơn mong đợi, recommend mạnh!'])
                        [FLOOR(RANDOM() * 4 + 1)::INT]
                    WHEN 4 THEN (ARRAY[
                        'Sản phẩm tốt, đóng gói cẩn thận.',
                        'Khá hài lòng, chỉ hơi chậm giao.',
                        'Chất lượng tốt so với giá.',
                        'Ổn, sẽ cân nhắc mua lại.'])
                        [FLOOR(RANDOM() * 4 + 1)::INT]
                    WHEN 3 THEN (ARRAY[
                        'Bình thường, không có gì đặc biệt.',
                        'Tạm được, giá hơi cao.',
                        'Sản phẩm vừa ý, giao hàng hơi lâu.'])
                        [FLOOR(RANDOM() * 3 + 1)::INT]
                    WHEN 2 THEN (ARRAY[
                        'Không hài lòng lắm, chất lượng kém.',
                        'Sản phẩm khác mô tả.',
                        'Đóng gói sơ sài, hàng bị xước.'])
                        [FLOOR(RANDOM() * 3 + 1)::INT]
                    ELSE (ARRAY[
                        'Rất thất vọng. Sản phẩm lỗi.',
                        'Hàng giả, không đúng mô tả.',
                        'Giao sai hàng, dịch vụ kém.'])
                        [FLOOR(RANDOM() * 3 + 1)::INT]
                END;

                _source := (ARRAY['Shopee', 'Lazada', 'Tiki', 'Website'])[
                    FLOOR(RANDOM() * 4 + 1)::INT];

                -- Trigger trg_unique_review đảm bảo 1 review/customer/product/source
                INSERT INTO product_review (product_id, customer_id, rating, review_text, source_system,
                                            created_at, updated_at, is_active)
                VALUES (_product_id, _customer_id, _rating, _review_text, _source,
                        NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE);

                RAISE NOTICE 'Review: KH % → SP % (% sao)', _customer_id, _product_id, _rating;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Review thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong ReviewGenerator: {e}")
            return False

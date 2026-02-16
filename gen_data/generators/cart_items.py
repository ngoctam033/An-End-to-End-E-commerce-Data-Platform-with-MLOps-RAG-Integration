from .base import BaseGenerator
from logger import logger

class CartItemsGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _cart_id BIGINT;
                _product_id BIGINT;
                _product_price NUMERIC(12,2);
                _quantity INT;
                _num_items INT;
                _added INT := 0;
                i INT;
            BEGIN
                -- Tìm giỏ hàng active
                SELECT id INTO _cart_id
                FROM cart WHERE status = 'active' AND is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                IF _cart_id IS NULL THEN
                    RAISE NOTICE 'Không có giỏ hàng active.';
                    RETURN;
                END IF;

                _num_items := FLOOR(RANDOM() * 3 + 1)::INT; -- 1-3 sản phẩm

                FOR i IN 1.._num_items LOOP
                    -- Chọn sản phẩm active chưa có trong giỏ
                    SELECT p.id, p.price INTO _product_id, _product_price
                    FROM product p
                    WHERE p.is_active = TRUE
                      AND NOT EXISTS (
                          SELECT 1 FROM cart_items ci
                          WHERE ci.cart_id = _cart_id AND ci.product_id = p.id
                      )
                    ORDER BY RANDOM() LIMIT 1;

                    IF _product_id IS NULL THEN
                        EXIT; -- Hết sản phẩm có thể thêm
                    END IF;

                    _quantity := FLOOR(RANDOM() * 3 + 1)::INT;

                    -- Trigger trg_calc_cart_item sẽ tự snapshot giá và tính amount
                    INSERT INTO cart_items (cart_id, product_id, quantity, unit_price, amount, added_at, updated_at, is_active)
                    VALUES (_cart_id, _product_id, _quantity, _product_price, _product_price * _quantity, NOW()::TIMESTAMP, NOW()::TIMESTAMP, TRUE);

                    _added := _added + 1;
                END LOOP;

                -- Trigger trg_update_cart_totals tự cập nhật total_amount và item_count

                RAISE NOTICE 'Thêm % sản phẩm vào giỏ ID: %', _added, _cart_id;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Cart Items thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong CartItemsGenerator: {e}")
            return False

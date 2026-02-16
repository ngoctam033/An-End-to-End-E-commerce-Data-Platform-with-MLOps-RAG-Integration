from .base import BaseGenerator
from logger import logger

class DiscountGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _name VARCHAR(100);
                _type VARCHAR(50);
                _value NUMERIC(12,2);
                _applies_to VARCHAR(50);
                _source VARCHAR(50);
                _duration_days INT;
            BEGIN
                _type := (ARRAY['%', 'fixed'])[FLOOR(RANDOM() * 2 + 1)::INT];

                -- Trigger validate_discount_value: max 70% cho loại %
                IF _type = '%' THEN
                    _value := FLOOR(RANDOM() * 50 + 5); -- 5-54%
                ELSE
                    _value := FLOOR(RANDOM() * 500000 + 10000); -- 10,000 - 510,000 VND
                END IF;

                _applies_to := (ARRAY['product', 'category', 'order'])[
                    FLOOR(RANDOM() * 3 + 1)::INT];

                _source := (ARRAY['Shopee', 'Lazada', 'Tiki', 'Website'])[
                    FLOOR(RANDOM() * 4 + 1)::INT];

                _duration_days := FLOOR(RANDOM() * 30 + 3)::INT; -- 3-32 ngày

                _name := (ARRAY[
                    'Flash Sale', 'Giảm Sốc', 'Ưu Đãi Đặc Biệt', 'Deal Hot',
                    'Khuyến Mãi Cuối Tuần', 'Voucher Mới', 'Sale Tháng',
                    'Happy Hour', 'Combo Tiết Kiệm', 'Tri Ân Khách Hàng'
                ])[FLOOR(RANDOM() * 10 + 1)::INT]
                || ' ' || _source || ' '
                || CASE _type WHEN '%' THEN _value || '%' ELSE _value || 'đ' END;

                -- Trigger trg_check_discount_dates validate start < end
                INSERT INTO discount (name, discount_type, value, start_date, end_date,
                                      applies_to, source_system, is_active, created_at, updated_at)
                VALUES (_name, _type, _value, NOW()::TIMESTAMP, NOW()::TIMESTAMP + (_duration_days || ' days')::INTERVAL,
                        _applies_to, _source, TRUE, NOW()::TIMESTAMP, NOW()::TIMESTAMP);

                RAISE NOTICE 'Tạo khuyến mãi: % (%: %)', _name, _type, _value;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Discount thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong DiscountGenerator: {e}")
            return False

from .base import BaseGenerator
from logger import logger

class ReturnGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _order_id BIGINT;
                _customer_id BIGINT;
                _total_price NUMERIC(12,2);
                _reason VARCHAR(200);
                _return_type VARCHAR(50);
                _refund_amount NUMERIC(12,2);
            BEGIN
                -- Tìm đơn hàng Delivered/Completed chưa có yêu cầu trả hàng
                SELECT o.id, o.customer_id, o.total_price
                INTO _order_id, _customer_id, _total_price
                FROM orders o
                WHERE o.status IN ('Delivered', 'Completed')
                  AND NOT EXISTS (
                      SELECT 1 FROM order_return r WHERE r.order_id = o.id
                  )
                ORDER BY RANDOM() LIMIT 1;

                IF _order_id IS NULL THEN
                    RAISE NOTICE 'Không có đơn hàng nào đủ điều kiện để trả hàng.';
                    RETURN;
                END IF;

                _reason := (ARRAY[
                    'Sản phẩm lỗi', 'Khác mô tả', 'Giao nhầm hàng',
                    'Không hài lòng với chất lượng', 'Kích thước không phù hợp',
                    'Sản phẩm bị hư hỏng khi vận chuyển', 'Đổi ý kiến'
                ])[FLOOR(RANDOM() * 7 + 1)::INT];

                _return_type := (ARRAY['refund', 'exchange', 'repair'])[
                    FLOOR(RANDOM() * 3 + 1)::INT];

                -- Refund amount: 50-100% tổng đơn
                _refund_amount := ROUND((_total_price * (RANDOM() * 0.5 + 0.5))::NUMERIC, 2);

                -- Trigger trg_validate_return sẽ kiểm tra điều kiện
                INSERT INTO order_return (order_id, customer_id, reason, return_type, status, refund_amount,
                                          is_active, created_at, updated_at)
                VALUES (_order_id, _customer_id, _reason, _return_type, 'pending', _refund_amount,
                        TRUE, NOW()::TIMESTAMP, NOW()::TIMESTAMP);

                RAISE NOTICE 'Yêu cầu trả hàng cho Order ID: % - Lý do: %', _order_id, _reason;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Return Request thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong ReturnGenerator: {e}")
            return False

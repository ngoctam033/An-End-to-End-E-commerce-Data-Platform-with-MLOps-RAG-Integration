from .base import BaseGenerator
from logger import logger

class OrderStatusGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _order_id BIGINT;
                _current_status VARCHAR(50);
                _new_status VARCHAR(50);
                _customer_id BIGINT;
                _total_price NUMERIC(12,2);
                _payment_id BIGINT;
            BEGIN
                -- Tìm đơn hàng chưa hoàn tất
                SELECT id, status, customer_id, total_price, payment_id
                INTO _order_id, _current_status, _customer_id, _total_price, _payment_id
                FROM orders
                WHERE status NOT IN ('Completed', 'Cancelled', 'Refunded', 'Returned')
                  AND is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                IF _order_id IS NULL THEN
                    RAISE NOTICE 'Không có đơn hàng cần xử lý.';
                    RETURN;
                END IF;

                -- Xác định trạng thái kế tiếp
                _new_status := CASE _current_status
                    WHEN 'Pending' THEN
                        CASE WHEN RANDOM() < 0.9 THEN 'Confirmed' ELSE 'Cancelled' END
                    WHEN 'Confirmed' THEN 'Processing'
                    WHEN 'Processing' THEN 'Packed'
                    WHEN 'Packed' THEN 'Shipped'
                    WHEN 'Shipped' THEN 'Delivering'
                    WHEN 'Delivering' THEN
                        CASE WHEN RANDOM() < 0.85 THEN 'Delivered' ELSE 'DeliveryFailed' END
                    WHEN 'DeliveryFailed' THEN
                        CASE WHEN RANDOM() < 0.7 THEN 'Shipped' ELSE 'Returned' END
                    WHEN 'Delivered' THEN 'Completed'
                    ELSE NULL
                END;

                IF _new_status IS NULL THEN
                    RAISE NOTICE 'Không xác định trạng thái kế tiếp cho: %', _current_status;
                    RETURN;
                END IF;

                -- Cập nhật đơn hàng (trigger trg_order_status_check validates)
                UPDATE orders
                SET status = _new_status,
                    updated_at = NOW()::TIMESTAMP,
                    confirmed_at = CASE WHEN _new_status = 'Confirmed' THEN NOW()::TIMESTAMP ELSE confirmed_at END,
                    delivered_at = CASE WHEN _new_status = 'Delivered' THEN NOW()::TIMESTAMP ELSE delivered_at END,
                    cancelled_at = CASE WHEN _new_status = 'Cancelled' THEN NOW()::TIMESTAMP ELSE cancelled_at END,
                    cancel_reason = CASE WHEN _new_status = 'Cancelled' THEN
                        (ARRAY['Khách đổi ý', 'Tìm được giá rẻ hơn', 'Đặt nhầm', 'Không cần nữa', 'Chờ quá lâu'])[
                            FLOOR(RANDOM() * 5 + 1)::INT]
                        ELSE cancel_reason END
                WHERE id = _order_id;

                -- Log trạng thái
                INSERT INTO order_status_history (order_id, status, changed_at, changed_by, is_active)
                VALUES (_order_id, _new_status, NOW()::TIMESTAMP, 'System_Simulator', TRUE);

                -- Khi Completed: cập nhật total_spent & payment
                IF _new_status = 'Completed' THEN
                    UPDATE customers
                    SET total_spent = total_spent + _total_price
                    WHERE id = _customer_id;

                    UPDATE payment
                    SET payment_status = 'Completed', updated_at = NOW()::TIMESTAMP
                    WHERE id = _payment_id;

                    -- Cập nhật shipment
                    UPDATE shipment
                    SET shipping_status = 'Delivered', actual_delivery = NOW()::TIMESTAMP, updated_at = NOW()::TIMESTAMP
                    WHERE id = (SELECT shipping_id FROM orders WHERE id = _order_id);
                END IF;

                -- Khi Cancelled: cập nhật payment
                IF _new_status = 'Cancelled' THEN
                    UPDATE payment
                    SET payment_status = 'Cancelled', updated_at = NOW()::TIMESTAMP
                    WHERE id = _payment_id;
                END IF;

                -- Khi Shipped: cập nhật shipment
                IF _new_status = 'Shipped' THEN
                    UPDATE shipment
                    SET shipping_status = 'InTransit', updated_at = NOW()::TIMESTAMP
                    WHERE id = (SELECT shipping_id FROM orders WHERE id = _order_id);
                END IF;

                -- Khi DeliveryFailed: tăng delivery_attempts
                IF _new_status = 'DeliveryFailed' THEN
                    UPDATE shipment
                    SET shipping_status = 'Failed',
                        delivery_attempts = delivery_attempts + 1,
                        updated_at = NOW()::TIMESTAMP
                    WHERE id = (SELECT shipping_id FROM orders WHERE id = _order_id);
                END IF;

                RAISE NOTICE 'Order ID: % chuyển từ % → %', _order_id, _current_status, _new_status;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Order Status thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong OrderStatusGenerator: {e}")
            return False

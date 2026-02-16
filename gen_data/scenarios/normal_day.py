import time
from datetime import datetime
import random

from logger import logger
from .base import BaseSimulator

class NormalDaySimulator(BaseSimulator):
    """
    Kịch bản: Một ngày kinh doanh bình thường (chế độ liên tục).
    
    Mô phỏng đầy đủ vòng đời nghiệp vụ thương mại điện tử:
    - Khách hàng mới đăng ký
    - Hoạt động duyệt web, tìm kiếm, xem sản phẩm
    - Thêm sản phẩm vào wishlist
    - Giỏ hàng: tạo, thêm sản phẩm, checkout
    - Đơn hàng trực tiếp: tạo, thêm items
    - Xử lý trạng thái đơn hàng (Pending → Completed)
    - Đánh giá sản phẩm
    - Trả hàng/hoàn tiền
    - Tạo khuyến mãi
    - Bổ sung tồn kho
    """
    def run(self, **kwargs):
        logger.info("=== BẮT ĐẦU KỊCH BẢN: NGÀY THƯỜNG - ĐẦY ĐỦ NGHIỆP VỤ ===")
        
        today_date = datetime.now().date()
        cycle_count = 0
        
        while True:
            # 1. Lấy thời gian mô phỏng
            current_sim_time = self.time_manager.get_datetime()
            sim_timestamp = current_sim_time.get('timestamp')
            
            # Kiểm tra điều kiện dừng
            if sim_timestamp and sim_timestamp.date() >= today_date:
                logger.info(f"Đã hoàn thành mô phỏng đến {today_date}. Kết thúc.")
                break

            cycle_count += 1
            logger.info(f"--- Chu kỳ #{cycle_count} | {current_sim_time['formatted']} ---")
            sim_time_sql = f"'{current_sim_time['formatted']}'"
            
            try:
                # ============================================================
                # PHASE 1: KHÁCH HÀNG & HOẠT ĐỘNG
                # ============================================================
                
                # 1.1 Tạo khách hàng mới (30%)
                if random.random() < 0.3:
                    self._run_gen(self.customer_gen, sim_time_sql, "Customer")

                # 1.2 Ghi log hoạt động khách hàng (70%)
                if random.random() < 0.7:
                    self._run_gen(self.activity_log_gen, sim_time_sql, "Activity Log")

                # 1.3 Thêm vào wishlist (15%)
                if random.random() < 0.15:
                    self._run_gen(self.wishlist_gen, sim_time_sql, "Wishlist")

                # ============================================================
                # PHASE 2: GIỎ HÀNG (CART FLOW)
                # ============================================================

                # 2.1 Tạo giỏ hàng mới (25%)
                if random.random() < 0.25:
                    self._run_gen(self.cart_gen, sim_time_sql, "Cart")

                # 2.2 Thêm sản phẩm vào giỏ (40%)
                if random.random() < 0.4:
                    self._run_gen(self.cart_items_gen, sim_time_sql, "Cart Items")

                # 2.3 Checkout giỏ hàng → tạo đơn (20%)
                if random.random() < 0.2:
                    self._run_gen(self.cart_checkout_gen, sim_time_sql, "Cart Checkout")

                # ============================================================
                # PHASE 3: ĐƠN HÀNG TRỰC TIẾP (KHÔNG QUA GIỎ)
                # ============================================================

                # 3.1 Tạo đơn hàng trực tiếp (30%)
                if random.random() < 0.3:
                    num_orders = random.randint(1, 2)
                    for _ in range(num_orders):
                        self._run_gen(self.order_gen, sim_time_sql, "Order")

                # 3.2 Thêm items cho đơn hàng trống (luôn chạy)
                self._run_gen(self.order_item_gen, sim_time_sql, "Order Items")

                # ============================================================
                # PHASE 4: XỬ LÝ ĐƠN HÀNG (ORDER LIFECYCLE)
                # ============================================================

                # 4.1 Chuyển trạng thái đơn hàng (40%)
                if random.random() < 0.4:
                    # Xử lý 1-3 đơn mỗi lần
                    num_status = random.randint(1, 3)
                    for _ in range(num_status):
                        self._run_gen(self.order_status_gen, sim_time_sql, "Order Status")

                # ============================================================
                # PHASE 5: SAU BÁN HÀNG (POST-SALE)
                # ============================================================

                # 5.1 Đánh giá sản phẩm (10%)
                if random.random() < 0.1:
                    self._run_gen(self.review_gen, sim_time_sql, "Review")

                # 5.2 Yêu cầu trả hàng (5%)
                if random.random() < 0.05:
                    self._run_gen(self.return_gen, sim_time_sql, "Return")

                # ============================================================
                # PHASE 6: VẬN HÀNH (OPERATIONS)
                # ============================================================

                # 6.1 Tạo khuyến mãi mới (5%)
                if random.random() < 0.05:
                    self._run_gen(self.discount_gen, sim_time_sql, "Discount")

                # 6.2 Bổ sung tồn kho (10%)
                if random.random() < 0.1:
                    self._run_gen(self.inventory_gen, sim_time_sql, "Inventory Restock")

                # 6.3 Ghi log thay đổi tồn kho (15%)
                if random.random() < 0.15:
                    self._run_gen(self.inventory_log_gen, sim_time_sql, "Inventory Log")

            except Exception as e:
                logger.error(f"Lỗi trong chu kỳ mô phỏng #{cycle_count}: {e}")
                time.sleep(10)
            
            time.sleep(10)  # Nghỉ 10s giữa các chu kỳ

        logger.info(f"=== KẾT THÚC KỊCH BẢN: Đã chạy {cycle_count} chu kỳ ===")

    def _run_gen(self, generator, sim_time_sql, name):
        """Helper: thay thế NOW() bằng thời gian mô phỏng và chạy generator."""
        try:
            # Reset SQL về bản gốc (trong __init__) rồi replace
            generator.__init__()
            generator.sql_command = generator.sql_command.replace("NOW()", sim_time_sql)
            generator.generate()
        except Exception as e:
            logger.error(f"Lỗi khi chạy {name}: {e}")
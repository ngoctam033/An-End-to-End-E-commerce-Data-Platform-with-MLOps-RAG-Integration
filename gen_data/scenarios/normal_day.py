import time
import random

from logger import logger
from .base import BaseSimulator

class NormalDaySimulator(BaseSimulator):
    """
    Kịch bản: Một ngày kinh doanh bình thường.
    - Chạy liên tục (while True).
    - Khách hàng mới xuất hiện rải rác.
    - Đơn hàng phát sinh theo nhịp độ vừa phải.
    """
    def run(self, **kwargs):
        logger.info("=== BẮT ĐẦU KỊCH BẢN: NGÀY THƯỜNG ỔN ĐỊNH (CHẾ ĐỘ LIÊN TỤC) ===")
        
        while True:
            # Lấy thời gian từ time_manager
            current_sim_time = self.time_manager.get_datetime()
            logger.info(f"Chu kỳ xử lý tại thời điểm: {current_sim_time['formatted']}")
            sim_time_sql = f"'{current_sim_time['formatted']}'"
            try:
                # 1. Thỉnh thoảng mới có khách hàng mới (tỉ lệ 30%)
                if random.random() < 0.3:
                    self.customer_gen.sql_command = self.customer_gen.sql_command.replace("NOW()", sim_time_sql)
                    self.customer_gen.generate()
                
                # 2. Đơn hàng phát sinh (luôn có trong mỗi chu kỳ)
                self.order_gen.sql_command = self.order_gen.sql_command.replace("NOW()", sim_time_sql)
                self.order_gen.generate()
                self.order_item_gen.sql_command = self.order_item_gen.sql_command.replace("NOW()", sim_time_sql)
                self.order_item_gen.generate()
                
                # 3. Cập nhật tồn kho (tỉ lệ 1%)
                if random.random() < 0.1:
                    self.inventory_gen.sql_command = self.inventory_gen.sql_command.replace("NOW()", sim_time_sql)
                    self.inventory_gen.generate()
                
            except Exception as e:
                logger.error(f"Lỗi xảy ra trong vòng lặp mô phỏng: {e}")
                time.sleep(10)  # 10 giây cho mục đích demo, có thể thay đổi thành 1 giây
            time.sleep(10)  # 10 giây cho mục đích demo, có thể thay đổi thành 1 giây
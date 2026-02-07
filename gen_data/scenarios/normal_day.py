import time
from datetime import datetime
import random

from logger import logger
from .base import BaseSimulator

class NormalDaySimulator(BaseSimulator):
    """
    Kịch bản: Một ngày kinh doanh bình thường.
    - Chạy cho đến khi thời gian mô phỏng chạm đến ngày hôm nay.
    - Khách hàng mới xuất hiện ngẫu nhiên.
    - Đơn hàng phát sinh không liên tục, số lượng mỗi lần từ 1-3 đơn.
    """
    def run(self, **kwargs):
        logger.info("=== BẮT ĐẦU KỊCH BẢN: NGÀY THƯỜNG ỔN ĐỊNH (CHẾ ĐỘ LIÊN TỤC) ===")
        
        # Lấy ngày hiện tại của hệ thống để làm mốc dừng
        today_date = datetime.now().date()
        
        while True:
            # 1. Lấy thời gian mô phỏng hiện tại từ time_manager
            current_sim_time = self.time_manager.get_datetime()
            sim_timestamp = current_sim_time.get('timestamp')
            
            # KIỂM TRA ĐIỀU KIỆN DỪNG: 
            # Nếu ngày của thời gian mô phỏng >= ngày hôm nay thì thoát vòng lặp
            if sim_timestamp and sim_timestamp.date() >= today_date:
                logger.info(f"Đã hoàn thành mô phỏng đến mốc ngày hôm nay ({today_date}). Kết thúc.")
                break

            logger.info(f"Chu kỳ xử lý tại thời điểm: {current_sim_time['formatted']}")
            sim_time_sql = f"'{current_sim_time['formatted']}'"
            try:
                # 2. Tạo khách hàng mới (tỉ lệ 30%)
                if random.random() < 0.3:
                    self.customer_gen.sql_command = self.customer_gen.sql_command.replace("NOW()", sim_time_sql)
                    self.customer_gen.generate()
                
                # 3. Phát sinh đơn hàng ngẫu nhiên
                # Tỉ lệ 50% cơ hội có đơn hàng trong mỗi chu kỳ xử lý
                if random.random() < 0.5:
                    num_orders = random.randint(1, 3)
                    
                    for _ in range(num_orders):
                        # Cập nhật thời gian vào câu lệnh SQL và thực thi
                        self.order_gen.sql_command = self.order_gen.sql_command.replace("NOW()", sim_time_sql)
                        self.order_gen.generate()
                        
                        self.order_item_gen.sql_command = self.order_item_gen.sql_command.replace("NOW()", sim_time_sql)
                        self.order_item_gen.generate()
                
                # 4. Cập nhật tồn kho (tỉ lệ 10%)
                if random.random() < 0.1:
                    self.inventory_gen.sql_command = self.inventory_gen.sql_command.replace("NOW()", sim_time_sql)
                    self.inventory_gen.generate()
                
            except Exception as e:
                logger.error(f"Lỗi xảy ra trong vòng lặp mô phỏng: {e}")
                # time.sleep(1)
            # time.sleep(1)
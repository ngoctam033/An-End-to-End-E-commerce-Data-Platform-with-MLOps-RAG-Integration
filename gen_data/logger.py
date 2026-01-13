import logging
import os
from datetime import datetime

class CustomLogger:
    def __init__(self, name="app_logger", log_dir="logs", level=logging.INFO):
        """
        Khởi tạo logger tùy chỉnh.
        
        Args:
            name (str): Tên của logger.
            log_dir (str): Đường dẫn thư mục lưu file log.
            level (int): Mức độ log tối thiểu (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.log_dir = log_dir
        
        # Tạo thư mục logs nếu chưa tồn tại
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        # Định dạng log: Thời gian - Tên logger - Mức độ - Thông báo
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. Handler ghi ra file (FileHandler)
        # Tên file log sẽ bao gồm ngày tháng năm để dễ quản lý (VD: logs/2023-10-27_app.log)
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_filename = os.path.join(self.log_dir, f"{current_date}_app.log")
        
        # encoding='utf-8' để hỗ trợ ghi tiếng Việt
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)

        # 2. Handler ghi ra màn hình console (StreamHandler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)

        # Kiểm tra để tránh thêm handler nhiều lần nếu logger đã được khởi tạo trước đó
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger

logger_setup = CustomLogger()
logger = logger_setup.get_logger()
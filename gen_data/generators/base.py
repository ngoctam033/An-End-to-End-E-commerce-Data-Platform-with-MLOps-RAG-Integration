from abc import ABC, abstractmethod
from core.db import db
from logger import logger

class BaseGenerator(ABC):
    def __init__(self):
        self.db = db
        self.sql_command = None
        self.params = None

    @abstractmethod
    def generate(self):
        """
        Hàm generate thực thi câu lệnh SQL được lưu trong self.sql_command
        để tạo record trong database.
        """
        if self.sql_command:
            # Đưa thuộc tính sql_command vào hàm execute_query
            result = self.db.execute_query(self.sql_command, self.params)
            return result
        else:
            logger.info("Chưa có câu lệnh SQL nào được thiết lập cho generator này.")
            return False
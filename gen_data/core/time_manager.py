from datetime import datetime, timedelta
import os

from logger import logger

class TimeManager:
    """
    Class quản lý thời gian giả lập cho kịch bản đơn hàng - tồn kho.
    Tự động tăng thời gian và lưu trạng thái vào file.
    """
    
    def __init__(self, start_date=None, state_file="./simulation_time.txt"):
        """
        Khởi tạo TimeManager.
        
        Args:
            start_date (datetime): Thời gian bắt đầu giả lập (mặc định 01/01/2023).
            state_file (str): Tên file dùng để lưu trữ trạng thái thời gian.
        """
        self.default_start = start_date if start_date else datetime(2023, 1, 1)
        self.state_file = state_file
        self.current_time = self._load_state()

    def _load_state(self):
        """
        Đọc thời gian từ file. Nếu file không tồn tại, trả về thời gian khởi đầu.
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    time_str = f.read().strip()
                    if time_str:
                        return datetime.fromisoformat(time_str)
            except Exception:
                # Nếu file lỗi, lờ đi và dùng mặc định
                pass
        
        return self.default_start

    def _save_state(self):
        """Lưu thời gian hiện tại vào file."""
        try:
            with open(self.state_file, 'w') as f:
                f.write(self.current_time.isoformat())
            logger.debug(f"Saved simulation time state: {self.current_time.isoformat()}")
        except Exception as e:
            logger.error(f"Error saving time state: {e}")

    def get_datetime(self):
        """
        Mỗi lần gọi hàm này, thời gian giả lập sẽ tăng thêm 1 phút.
        
        Returns:
            dict: Thông tin thời gian mới.
        """
        # Tăng thời gian thêm 10 phút
        self.current_time += timedelta(minutes=10)
        
        # Lưu trạng thái ngay lập tức
        self._save_state()
        
        now = self.current_time
        
        # Tạo biến chứa thông tin chi tiết
        time_info = {
            "hour": now.hour,
            "minute": now.minute,
            "day": now.day,
            "month": now.month,
            "year": now.year,
            "formatted": now.strftime('%Y-%m-%d %H:%M:%S'),
            "timestamp": now
        }
        
        return time_info

    def reset_simulation(self):
        """
        Reset thời gian về mốc khởi đầu (01/01/2023).
        Dùng khi muốn chạy lại kịch bản từ đầu.
        """
        self.current_time = self.default_start
        self._save_state()
from pathlib import Path
from typing import Union, List, Set, Dict, Any

class PathNode:
    def __init__(self, base: Union[str, Path], parts: List[str], manager: 'PathManager'):
        self._base = Path(base)
        self._parts = parts
        self._manager = manager

    def __getattr__(self, name: str):
        """
        Bắt mọi attribute và ủy quyền việc kiểm tra/tạo node mới cho manager.
        """
        if name.startswith("_"):
            raise AttributeError

        # Gọi đến phương thức xử lý của PathManager
        return self._manager.create_child_node(self, name)

    def get(self) -> str:
        """
        Trả về đối tượng Path cuối cùng từ thư viện pathlib.
        """
        return str(self._base.joinpath(*self._parts))

    def get_table(self) -> str:
        """
        Trả về tên bảng theo định dạng Iceberg (catalog.database.table)
        """
        return ".".join(self._parts)

    def __str__(self):
        return str(self.get())

    def __repr__(self):
        return f"PathNode({self.get()})"


class PathManager(PathNode):
    def __init__(self, base_dir: Union[str, Path] = "", allowed_paths: Dict[str, Any] = None):
        """
        Khởi tạo PathManager với cấu trúc cây thư mục được phép.
        
        Args:
            base_dir: Thư mục gốc.
            allowed_paths: Dictionary biểu diễn cấu trúc cây (e.g., {"data": {"raw": {}}})
        """
        # Lưu trữ cấu trúc cây các đường dẫn được phép
        self._allowed_paths = allowed_paths if allowed_paths else {}
        
        # Khởi tạo node gốc, với manager chính là bản thân (self)
        super().__init__(base=base_dir, parts=[], manager=self)

    def create_child_node(self, current_node: PathNode, name: str) -> PathNode:
        """
        Kiểm tra tính hợp lệ bằng cách duyệt qua cấu trúc dictionary (tree).
        """
        new_parts = current_node._parts + [name]
        
        # Duyệt cây từ gốc để kiểm tra xem đường dẫn mới có hợp lệ không
        cursor = self._allowed_paths
        for part in new_parts:
            if isinstance(cursor, dict) and part in cursor:
                cursor = cursor[part]
            else:
                current_chain = ".".join(new_parts)
                raise AttributeError(
                    f"Lỗi truy cập: Đường dẫn '{current_chain}' không nằm trong cấu trúc được phép của PathManager."
                )

        # Nếu hợp lệ, trả về một PathNode mới
        return PathNode(
            base=current_node._base,
            parts=new_parts,
            manager=self
        )
# Cấu trúc bảng của Iceberg
TABLES = [
    "orders", "order_items", "order_channel", "order_status_history",
    "product", "product_review", "sub_category", "category",
    "discount", "brand", "payment", "shipment", "logistics_partner",
    "shipping_method", "customers", "geo_location", "warehouse", "inventory"
]

GOLD_TABLES = [
    "daily_sales_mart", "customer_analytics_mart", "product_performance_mart",
    "logistics_mart", "inventory_snapshot_mart"
]

ALLOWED_PATHS = {
    "iceberg": {
        "raw": {table: {} for table in TABLES},
        "silver": {table: {} for table in TABLES},
        "gold": {table: {} for table in GOLD_TABLES}
    }
}
path_manager = PathManager(allowed_paths=ALLOWED_PATHS)

# THỬ NGHIỆM:
# Trường hợp đúng
# path_manager.lakehouse.raw.orders.get()}") 
    
# Trường hợp sai (gọi một folder không có trong danh sách)
# path_manager.lakehouse.temp_files.get())
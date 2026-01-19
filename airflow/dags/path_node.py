from pathlib import Path
from typing import Union, List, Set


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

    def get(self) -> Path:
        """
        Trả về đối tượng Path cuối cùng từ thư viện pathlib.
        """
        return self._base.joinpath(*self._parts)

    def __str__(self):
        return str(self.get())

    def __repr__(self):
        return f"PathNode({self.get()})"


class PathManager(PathNode):
    def __init__(self, base_dir: Union[str, Path] = "", allowed_paths: List[str] = None):
        # Lưu trữ danh sách trắng các đường dẫn được phép
        self._allowed_paths = set(allowed_paths) if allowed_paths else set()
        
        # Khởi tạo node gốc, với manager chính là bản thân (self)
        super().__init__(base=base_dir, parts=[], manager=self)

    def create_child_node(self, current_node: PathNode, name: str) -> PathNode:
        """
        Logic kiểm tra tính hợp lệ nằm tại đây thay vì nằm trong PathNode.
        """
        new_parts = current_node._parts + [name]
        current_chain = ".".join(new_parts)

        # Kiểm tra xem chuỗi hiện tại có thuộc danh sách trắng hoặc là tiền tố của danh sách trắng không
        is_valid = any(
            p == current_chain or p.startswith(current_chain + ".") 
            for p in self._allowed_paths
        )

        if not is_valid:
            raise AttributeError(
                f"Lỗi truy cập: Đường dẫn '{current_chain}' không nằm trong danh sách thiết lập của PathManager."
            )

        # Nếu hợp lệ, trả về một PathNode mới nhưng vẫn giữ tham chiếu đến manager này
        return PathNode(
            base=current_node._base,
            parts=new_parts,
            manager=self
        )

path_manager = PathManager()

# THỬ NGHIỆM:
# Trường hợp đúng
# path_manager.data.staging.users.get()}") 
    
# Trường hợp sai (gọi một folder không có trong danh sách)
# path_manager.data.temp_files.get())
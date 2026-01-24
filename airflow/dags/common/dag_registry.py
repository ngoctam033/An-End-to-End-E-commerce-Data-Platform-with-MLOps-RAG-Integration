import logging

# Cấu hình logger
logger = logging.getLogger("airflow.dag_registry")

class DagRegistry:
    """
    Class quản lý danh sách DAG ID trong bộ nhớ (In-memory list).
    Phù hợp cho mô hình Master DAG (tạo nhiều DAG từ một entry point).
    """
    def __init__(self):
        # Khởi tạo list rỗng để lưu trữ các dag_id
        self.dag_ids = [
            'ingest_orders_to_minio',
                'transform_orders_items_iceberg',
            # 'ingest_product_to_minio',
            # 'ingest_customers_to_minio',
            # 'ingest_category_to_minio',
            # 'ingest_geo_location_to_minio',
            # 'ingest_order_items_to_minio',
            # 'ingest_discount_to_minio',
            # 'ingest_order_channel_to_minio',
            # 'ingest_inventory_to_minio',
            # 'ingest_brand_to_minio',
            # 'ingest_logistics_partner_to_minio',
            # 'ingest_payment_to_minio',
            # 'ingest_shipment_to_minio',
            # 'ingest_warehouse_to_minio',
            # 'ingest_order_status_history_to_minio',
            # 'ingest_product_review_to_minio',
            # 'ingest_sub_category_to_minio',
            # 'ingest_shipping_method_to_minio',
        ]

    def get_dags(self):
        """
        Trả về danh sách các DAG ID hiện có trong list.
        Lưu ý: Chỉ gọi hàm này SAU KHI đã khai báo/đăng ký tất cả các DAG.
        """
        if not self.dag_ids:
            logger.warning("Danh sách DAG đang trống. Hãy đảm bảo bạn đã đăng ký DAG trước khi gọi get_dags().")
        return self.dag_ids

    # def register_dag(self, dag_id):
    #     """
    #     Thêm một dag_id (string) vào danh sách.
    #     Kiểm tra trùng lặp trước khi thêm.
    #     """
    #     if dag_id not in self.dag_ids:
    #         self.dag_ids.append(dag_id)
    #         logger.info(f"Đã thêm DAG ID vào registry: {dag_id}")
    #         return True
    #     return False
        
    # def dag_registration(self, dag_id):
    #     """
    #     Decorator để tự động đăng ký DAG ID ngay khi định nghĩa hàm.
        
    #     Ví dụ:
    #         @dag_registry.dag_registration("my_dag_id")
    #         def create_my_dag():
    #             ...
    #     """
    #     def decorator(func):
    #         # Logic đăng ký chạy ngay khi file được import/parse
    #         self.register_dag(dag_id)
    #         return func
    #     return decorator

    # def clear_registry(self):
    #     """Xóa danh sách (reset)."""
    #     self.dag_ids = []
    #     logger.info("Đã xóa danh sách DAG registry.")

# Khởi tạo instance singleton
dag_registry = DagRegistry()
from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from common.dag_registry import dag_registry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from airflow.utils.trigger_rule import TriggerRule

# Danh sách các DAG con cần chạy theo thứ tự tuần tự
CHILD_DAG_IDS = dag_registry.get_dags()
@dag(
    dag_id="master_orchestrator_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["parent", "sequential", "taskflow"],
    default_args={
        "owner": "airflow",
    }
)
def master_orchestrator():
    """
    DAG Master sử dụng decorator @dag để điều phối các DAG con chạy tuần tự.
    Dựa trên tài liệu Provider mới nhất, tham số truyền ngày là 'logical_date'.
    """
    
    prev_task = None

    # Duyệt qua danh sách ID để tạo các task Trigger tương ứng
    for child_id in CHILD_DAG_IDS:
        current_trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{child_id}",
            trigger_dag_id=child_id,
            # Sử dụng 'logical_date' theo đúng tài liệu Airflow Provider bạn đã cung cấp
            logical_date="{{ logical_date }}",
            # Đợi DAG con hoàn thành trước khi chuyển sang task tiếp theo
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # Thiết lập quan hệ phụ thuộc tuần tự (Sequential)
        if prev_task:
            prev_task >> current_trigger_task
        
        # Cập nhật task trước đó cho vòng lặp sau
        prev_task = current_trigger_task

# Khởi tạo DAG
master_orchestrator()
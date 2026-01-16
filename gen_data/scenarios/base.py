from abc import ABC, abstractmethod

from logger import logger
from core.time_manager import TimeManager

from generators.customers import CustomersGenerator
from generators.orders import OrderGenerator
from generators.order_items import OrderItemsGenerator
from generators.inventory import InventoryGenerator

class BaseSimulator(ABC):
    """
    Lớp cơ sở cho các kịch bản mô phỏng thương mại điện tử.
    """
    def __init__(self):
        self.inventory_gen = InventoryGenerator()
        self.customer_gen = CustomersGenerator()
        self.order_gen = OrderGenerator()
        self.order_item_gen = OrderItemsGenerator()
        self.time_manager = TimeManager()

    @abstractmethod
    def run(self, **kwargs):
        """
        Phương thức trừu tượng để thực thi kịch bản.
        """
        pass
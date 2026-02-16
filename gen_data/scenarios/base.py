from abc import ABC, abstractmethod

from logger import logger
from core.time_manager import TimeManager

from generators.customers import CustomersGenerator
from generators.orders import OrderGenerator
from generators.order_items import OrderItemsGenerator
from generators.inventory import InventoryGenerator
from generators.cart import CartGenerator
from generators.cart_items import CartItemsGenerator
from generators.cart_checkout import CartCheckoutGenerator
from generators.order_status import OrderStatusGenerator
from generators.returns import ReturnGenerator
from generators.reviews import ReviewGenerator
from generators.wishlist import WishlistGenerator
from generators.activity_log import ActivityLogGenerator
from generators.discounts import DiscountGenerator
from generators.inventory_log import InventoryLogGenerator

class BaseSimulator(ABC):
    """
    Lớp cơ sở cho các kịch bản mô phỏng thương mại điện tử.
    Khởi tạo tất cả generators cần thiết cho việc mô phỏng.
    """
    def __init__(self):
        # Core generators
        self.customer_gen = CustomersGenerator()
        self.order_gen = OrderGenerator()
        self.order_item_gen = OrderItemsGenerator()
        self.inventory_gen = InventoryGenerator()

        # Cart flow
        self.cart_gen = CartGenerator()
        self.cart_items_gen = CartItemsGenerator()
        self.cart_checkout_gen = CartCheckoutGenerator()

        # Order lifecycle
        self.order_status_gen = OrderStatusGenerator()
        self.return_gen = ReturnGenerator()

        # Customer engagement
        self.review_gen = ReviewGenerator()
        self.wishlist_gen = WishlistGenerator()
        self.activity_log_gen = ActivityLogGenerator()

        # Promotion
        self.discount_gen = DiscountGenerator()

        # Operations
        self.inventory_log_gen = InventoryLogGenerator()

        # Time manager
        self.time_manager = TimeManager()

    @abstractmethod
    def run(self, **kwargs):
        """
        Phương thức trừu tượng để thực thi kịch bản.
        """
        pass
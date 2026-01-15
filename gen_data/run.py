from core.db import db
from logger import logger
from generators.customers import CustomersGenerator
from generators.orders import OrderGenerator

def main():    
    customers_generator = CustomersGenerator()
    order_generator = OrderGenerator()
    is_success = False
    try:
        customers_generator.generate()
        order_generator.generate()
    except Exception as e:
        logger.error(f"An error occurred during generation: {e}")

    if is_success == True:
        logger.info("=== Generation Process Completed ===")

if __name__ == "__main__":
    main()

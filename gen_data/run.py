from logger import logger
from scenarios.normal_day import NormalDaySimulator

def main(): 
    normal_day_simulator = NormalDaySimulator()
    is_success = False
    try:
        normal_day_simulator.run()
        is_success = True
    except Exception as e:
        logger.error(f"An error occurred during generation: {e}")

    if is_success == True:
        logger.info("=== Generation Process Completed ===")

if __name__ == "__main__":
    main()

import os
import logging
import time
from datetime import datetime
import schedule

# Import our modules
import config
from extract import extract_weather_data
from load import load_data_files, init_database
from transform import run_transformations
from visualize import create_all_visualizations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("weather_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('weather_pipeline')

def run_pipeline():
    """
    Run the complete ETL pipeline
    """
    start_time = time.time()
    logger.info("Starting weather data pipeline")
    
    try:
        # Ensure database is initialized
        init_database(config.DATABASE_PATH)
        
        # Step 1: Extract data
        logger.info("Step 1: Extracting weather data")
        results = extract_weather_data(config.CITIES)
        
        # Step 2: Load raw data into database
        logger.info("Step 2: Loading data into database")
        load_data_files(results, config.DATABASE_PATH)
        
        # Step 3: Transform data
        logger.info("Step 3: Data transformation")
        run_transformations(config.DATABASE_PATH)
        
        # Step 4: Create visualizations
        logger.info("Step 4: Creating visualizations")
        create_all_visualizations(
            config.DATABASE_PATH, 
            config.VISUALIZATION_OUTPUT_DIR,
            config.DARK_MODE
        )
        
        execution_time = time.time() - start_time
        logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

def schedule_pipeline():
    """
    Schedule the pipeline to run at regular intervals
    """
    # Run immediately on startup
    run_pipeline()
    
    # Schedule to run every X hours
    hours = config.COLLECTION_FREQUENCY
    logger.info(f"Scheduling pipeline to run every {hours} hours")
    
    schedule.every(hours).hours.do(run_pipeline)
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    # Create directories if they don't exist
    os.makedirs("raw_data", exist_ok=True)
    os.makedirs(config.VISUALIZATION_OUTPUT_DIR, exist_ok=True)
    
    logger.info("Weather data pipeline initialized")
    
    # Run the pipeline once or schedule it
    if os.environ.get("SCHEDULE_PIPELINE", "False").lower() == "true":
        schedule_pipeline()
    else:
        run_pipeline()
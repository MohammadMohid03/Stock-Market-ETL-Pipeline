import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from utils.logger import setup_logger

from etl.extract import fetch_stock_data
from etl.transform import clean_data, transform_stock_data
from etl.validation import validate_data
from etl.load import load_data

logger = setup_logger("MainPipeline")

# Configuration
DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
PERIOD = "1y" # Fetch 1 year of data

def get_tracked_tickers(db_path="sqlite:///stock_data.db"):
    """Fetch distinct tickers currently in the database to keep them updated."""
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_path)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DISTINCT Ticker FROM stock_metrics"))
            tickers = [row[0] for row in result]
            if not tickers:
                return DEFAULT_TICKERS
            return tickers
    except Exception as e:
        logger.warning(f"Could not read tickers from DB (might be first run or missing table): {e}")
        return DEFAULT_TICKERS

def run_pipeline():
    """
    Executes the full ETL pipeline: Extract, Transform, Validate, Load.
    """
    logger.info("="*50)
    logger.info("Starting up the Stock Market ETL Pipeline")
    logger.info("="*50)
    
    # 1. Extract
    tickers_to_fetch = get_tracked_tickers()
    logger.info(f">>> PHASE 1: EXTRACT (Tickers: {tickers_to_fetch})")
    raw_df = fetch_stock_data(tickers_to_fetch, period=PERIOD)
    if raw_df.empty:
        logger.error("Pipeline aborted: Extraction phase returned no data.")
        return

    # 2. Transform
    logger.info(">>> PHASE 2: TRANSFORM")
    cleaned_df = clean_data(raw_df)
    transformed_df = transform_stock_data(cleaned_df)
    
    # 3. Validate
    logger.info(">>> PHASE 3: VALIDATE")
    is_valid = validate_data(transformed_df)
    if not is_valid:
        logger.error("Pipeline aborted: Data validation failed.")
        return
        
    # 4. Load
    logger.info(">>> PHASE 4: LOAD")
    load_success = load_data(transformed_df)
    if load_success:
        logger.info("Pipeline completed successfully!")
    else:
        logger.error("Pipeline finished with errors during the Load phase.")
    logger.info("="*50)

def main():
    """
    Main function to run the process immediately once, then schedule it.
    """
    logger.info("Initializing application...")
    
    # Run once immediately on startup
    run_pipeline()
    
    # Setup Scheduler
    scheduler = BlockingScheduler()
    
    # Schedule to run every weekday (Mon-Fri) at 17:00 (5 PM) which is after market close
    trigger = CronTrigger(day_of_week='mon-fri', hour=17, minute=0, timezone='US/Eastern')
    
    scheduler.add_job(run_pipeline, trigger=trigger, id='daily_stock_etl', replace_existing=True)
    
    logger.info(f"Scheduler started. Next run scheduled for: {trigger}")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped manually.")

if __name__ == "__main__":
    main()

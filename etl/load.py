import pandas as pd
from sqlalchemy import create_engine
import os

logger = setup_logger("LoadModule")

# Default local database, overridden by environment variable in production
DEFAULT_DB_URL = os.environ.get("DATABASE_URL", "sqlite:///stock_data.db")
# SQLAlchemy 1.4+ requires postgresql:// instead of postgres://, which is often what Render provides
if DEFAULT_DB_URL.startswith("postgres://"):
    DEFAULT_DB_URL = DEFAULT_DB_URL.replace("postgres://", "postgresql://", 1)

def load_data(df: pd.DataFrame, db_name: str = DEFAULT_DB_URL, table_name: str = "stock_metrics", if_exists: str = "replace") -> bool:
    """
    Loads the validated dataframe into a SQLite database.
    
    Args:
        df (pd.DataFrame): The validated dataframe to load.
        db_name (str): SQLite connection string.
        table_name (str): Name of the table to insert data into.
        if_exists (str): How to behave if the table already exists ('fail', 'replace', 'append').
        
    Returns:
        bool: True if loading was successful, False otherwise.
    """
    logger.info(f"Starting data load into '{db_name}', table: '{table_name}'.")
    
    try:
        # Create a database engine
        engine = create_engine(db_name)
        
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        
        logger.info(f"Successfully loaded {len(df)} records into the database.")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load data into database: {e}")
        return False

if __name__ == "__main__":
    from etl.extract import fetch_stock_data
    from etl.transform import clean_data, transform_stock_data
    from etl.validation import validate_data
    
    # Test load standalone
    raw_data = fetch_stock_data(["TSLA"], period="5d")
    cleaned_data = clean_data(raw_data)
    transformed_data = transform_stock_data(cleaned_data)
    if validate_data(transformed_data):
        success = load_data(transformed_data)
        print(f"Load Success: {success}")

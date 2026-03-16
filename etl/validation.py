import pandas as pd
from utils.logger import setup_logger

logger = setup_logger("ValidationModule")

def validate_data(df: pd.DataFrame) -> bool:
    """
    Validates the transformed dataframe before loading it into the database.
    Checks for:
    1. Empty DataFrame.
    2. Missing values (nulls) in critical columns.
    3. Duplicate rows explicitly.
    4. Anomalies (e.g., negative stock prices).
    
    Args:
        df (pd.DataFrame): Transformed DataFrame.
        
    Returns:
        bool: True if passes validation, False otherwise.
    """
    logger.info("Starting data validation checks.")
    
    if df.empty:
        logger.error("Validation Failed: The DataFrame is empty.")
        return False
        
    # Check for missing values in critical columns
    critical_cols = ['Ticker', 'Date', 'Close']
    for col in critical_cols:
        if col not in df.columns:
            logger.error(f"Validation Failed: Missing critical column '{col}'.")
            return False
        if df[col].isnull().any():
            logger.error(f"Validation Failed: Found null values in critical column '{col}'.")
            return False
            
    # Check for duplicates based on Ticker and Date
    if df.duplicated(subset=['Ticker', 'Date']).any():
        logger.error("Validation Failed: Found duplicate records for the same Ticker and Date.")
        return False
        
    # Check for negative stock prices (Anomaly Detection)
    if (df['Close'] < 0).any():
        logger.error("Validation Failed: Found negative stock prices in the 'Close' column.")
        return False
        
    logger.info("Data passed all validation checks successfully.")
    return True

if __name__ == "__main__":
    from etl.extract import fetch_stock_data
    from etl.transform import clean_data, transform_stock_data
    # Test validation standalone
    raw_data = fetch_stock_data(["MSFT"], period="1mo")
    cleaned_data = clean_data(raw_data)
    transformed_data = transform_stock_data(cleaned_data)
    is_valid = validate_data(transformed_data)
    print(f"Validation Result: {is_valid}")

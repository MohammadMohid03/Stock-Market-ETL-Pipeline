import pandas as pd
from utils.logger import setup_logger

logger = setup_logger("TransformModule")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the extracted DataFrame by removing or filling null values.
    
    Args:
        df (pd.DataFrame): Raw DataFrame from extraction.
        
    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    logger.info("Starting data cleaning process.")
    initial_len = len(df)
    
    # Check for missing values
    missing_counts = df.isnull().sum()
    if missing_counts.any():
        logger.warning(f"Found missing values:\n{missing_counts[missing_counts > 0]}")
    
    # We will forward fill missing values as stock prices generally carry forward over weekends/holidays
    df_cleaned = df.fillna(method='ffill')
    
    # If there are still any nulls (e.g., at the very beginning), we'll backward fill
    df_cleaned = df_cleaned.fillna(method='bfill')
    
    # Drop rows that still have NaNs (if any extreme cases)
    df_cleaned = df_cleaned.dropna()
    
    final_len = len(df_cleaned)
    logger.info(f"Data cleaning finished. Rows before: {initial_len}, Rows after: {final_len}")
    return df_cleaned

def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the cleaned DataFrame by adding analytical metrics.
    Calculates: moving averages (7-day, 30-day), daily % change, and volatility score.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        
    Returns:
        pd.DataFrame: Transformed DataFrame with new feature columns.
    """
    logger.info("Starting data transformation (adding metrics).")
    
    # Ensure data is sorted by Ticker and Date
    df = df.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)
    
    transformed_dfs = []
    
    # Process each ticker separately to avoid calculating metrics across different companies
    for ticker in df['Ticker'].unique():
        logger.info(f"Transforming data for '{ticker}'...")
        ticker_df = df[df['Ticker'] == ticker].copy()
        
        # Calculate Daily % Change based on the Close price
        ticker_df['Daily_Return_Pct'] = ticker_df['Close'].pct_change() * 100
        
        # Calculate Moving Averages (7-day and 30-day)
        # Using rolling window based on number of rows (trading days)
        ticker_df['MA_7_Days'] = ticker_df['Close'].rolling(window=7, min_periods=1).mean()
        ticker_df['MA_30_Days'] = ticker_df['Close'].rolling(window=30, min_periods=1).mean()
        
        # Calculate Volatility Score (30-day rolling standard deviation of daily returns)
        ticker_df['Volatility_30_Days'] = ticker_df['Daily_Return_Pct'].rolling(window=30, min_periods=1).std()
        
        # Fill resulting NaNs from pct_change/rolling with 0 (optional, or leave as NaN)
        ticker_df = ticker_df.fillna(0)
        
        transformed_dfs.append(ticker_df)
        
    # Combine back
    final_df = pd.concat(transformed_dfs, ignore_index=True)
    logger.info("Transformation complete. Added metrics: Daily_Return_Pct, MA_7_Days, MA_30_Days, Volatility_30_Days.")
    
    return final_df

if __name__ == "__main__":
    from etl.extract import fetch_stock_data
    # Test transformation standalone
    logger.info("Running standalone transformation test...")
    raw_data = fetch_stock_data(["AAPL", "TSLA"], period="6mo")
    if not raw_data.empty:
        cleaned_data = clean_data(raw_data)
        transformed_data = transform_stock_data(cleaned_data)
        print(transformed_data[['Date', 'Ticker', 'Close', 'MA_7_Days', 'Daily_Return_Pct', 'Volatility_30_Days']].tail())

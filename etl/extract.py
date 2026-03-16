import yfinance as yf
import pandas as pd
from typing import List
from utils.logger import setup_logger

logger = setup_logger("ExtractModule")

def fetch_stock_data(tickers: List[str], period: str = "1y") -> pd.DataFrame:
    """
    Extracts historical stock data for a list of tickers using yfinance.
    
    Args:
        tickers (List[str]): List of stock ticker symbols (e.g., ['AAPL', 'GOOGL']).
        period (str): The historical period to fetch data for (e.g., '1mo', '1y', 'max').
        
    Returns:
        pd.DataFrame: A pandas DataFrame containing the combined historical data.
    """
    logger.info(f"Starting data extraction for tickers: {tickers} over period: '{period}'")
    all_data = []

    for ticker in tickers:
        try:
            logger.info(f"Fetching data for '{ticker}'...")
            stock = yf.Ticker(ticker)
            # Fetch historical data
            df = stock.history(period=period)
            
            if df.empty:
                logger.warning(f"No data returned for '{ticker}'.")
                continue
                
            # Reset index to make Date a column
            df = df.reset_index()
            # Convert timezone-aware datetime to timezone-naive (sqlite compatibility)
            if df['Date'].dt.tz is not None:
                df['Date'] = df['Date'].dt.tz_localize(None)
                
            # Add Ticker column
            df['Ticker'] = ticker
            
            all_data.append(df)
            logger.info(f"Successfully fetched {len(df)} records for '{ticker}'.")
            
        except Exception as e:
            logger.error(f"Error fetching data for ticker '{ticker}': {e}")
            
    if not all_data:
        logger.error("No data fetched for any tickers.")
        return pd.DataFrame()
        
    # Combine all individual dataframes into one
    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Extraction complete. Total records fetched: {len(combined_df)}")
    
    return combined_df

if __name__ == "__main__":
    # Test the extraction standalone
    test_tickers = ["AAPL", "GOOGL"]
    data = fetch_stock_data(test_tickers, period="5d")
    print(data.head())

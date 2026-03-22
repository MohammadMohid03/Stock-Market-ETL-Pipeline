import pandas as pd
from typing import List
from utils.logger import setup_logger

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ModuleNotFoundError:
    HAS_YFINANCE = False

logger = setup_logger("ExtractModule")

def _fetch_from_stooq(ticker: str) -> pd.DataFrame:
    """Fallback data source when yfinance is unavailable."""
    url = f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"
    df = pd.read_csv(url)
    if df.empty or 'Date' not in df.columns:
        return pd.DataFrame()

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Close'])
    df['Ticker'] = ticker
    return df

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

    if not HAS_YFINANCE:
        logger.warning("yfinance is not installed. Falling back to Stooq CSV source.")

    for ticker in tickers:
        try:
            logger.info(f"Fetching data for '{ticker}'...")
            if HAS_YFINANCE:
                stock = yf.Ticker(ticker)
                # Fetch historical data
                df = stock.history(period=period)
            else:
                # Period is ignored for fallback source.
                df = _fetch_from_stooq(ticker)
            
            if df.empty:
                logger.warning(f"No data returned for '{ticker}'.")
                continue
                
            # Reset index to make Date a column
            if 'Date' not in df.columns:
                df = df.reset_index()
            # Convert timezone-aware datetime to timezone-naive (sqlite compatibility)
            if pd.api.types.is_datetime64_any_dtype(df['Date']) and getattr(df['Date'].dt, 'tz', None) is not None:
                df['Date'] = df['Date'].dt.tz_localize(None)
                
            # Add Ticker column
            df['Ticker'] = ticker
            
            all_data.append(df)
            logger.info(f"Successfully fetched {len(df)} records for '{ticker}'.")
            
        except Exception as e:
            logger.error(f"Error fetching data for ticker '{ticker}' via primary source: {e}")
            # If yfinance path failed at runtime, try fallback source per ticker.
            if HAS_YFINANCE:
                try:
                    df = _fetch_from_stooq(ticker)
                    if not df.empty:
                        all_data.append(df)
                        logger.info(f"Fetched {len(df)} fallback records for '{ticker}'.")
                except Exception as fallback_error:
                    logger.error(f"Fallback source also failed for '{ticker}': {fallback_error}")
            
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

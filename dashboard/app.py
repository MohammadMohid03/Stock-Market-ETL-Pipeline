import streamlit as st
import pandas as pd
import sqlite3
import os
import sys

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ModuleNotFoundError:
    HAS_PLOTLY = False

# Ensure the root folder is in the path to import utils (if needed)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

st.set_page_config(page_title="Stock Market Dashboard", layout="wide", page_icon="📈")

# Database connection via SQLAlchemy
DEFAULT_DB_URL = os.environ.get("DATABASE_URL", "sqlite:///stock_data.db")
if DEFAULT_DB_URL.startswith("postgres://"):
    DEFAULT_DB_URL = DEFAULT_DB_URL.replace("postgres://", "postgresql://", 1)

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
INITIAL_BOOTSTRAP_PERIOD = "3mo"
MAX_POINTS_PER_TICKER = 180

@st.cache_data(ttl=3600) # Cache data for 1 hour to prevent constant DB hits
def load_data_from_db():
    try:
        # If it's a local SQLite file, ensure it exists before querying
        if DEFAULT_DB_URL.startswith("sqlite") and not os.path.exists(DEFAULT_DB_URL.replace("sqlite:///", "")):
             return pd.DataFrame()
             
        query = "SELECT * FROM stock_metrics"
        if DEFAULT_DB_URL.startswith("sqlite"):
            db_path = DEFAULT_DB_URL.replace("sqlite:///", "", 1)
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql_query(query, conn)
        else:
            from sqlalchemy import create_engine
            engine = create_engine(DEFAULT_DB_URL)
            df = pd.read_sql(query, engine)
        
        # Ensure Date is parsed correctly
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            
        return df
    except Exception as e:
        if "no such table" in str(e).lower():
            return pd.DataFrame()
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

def _limit_points_per_ticker(df: pd.DataFrame, max_points: int = MAX_POINTS_PER_TICKER) -> pd.DataFrame:
    """Limit chart data points per ticker to keep rendering fast on Streamlit Cloud."""
    if df.empty or "Ticker" not in df.columns or "Date" not in df.columns:
        return df
    return (
        df.sort_values(by=["Ticker", "Date"]) 
          .groupby("Ticker", group_keys=False)
          .tail(max_points)
          .reset_index(drop=True)
    )

def run_initial_etl_bootstrap() -> tuple[bool, str]:
    """Run a one-time ETL load so hosted deployments have starter data."""
    try:
        from etl.extract import fetch_stock_data
        from etl.transform import clean_data, transform_stock_data
        from etl.validation import validate_data
        from etl.load import load_data

        raw_data = fetch_stock_data(DEFAULT_TICKERS, period=INITIAL_BOOTSTRAP_PERIOD)
        if raw_data.empty:
            return False, "Initial data load failed: no data returned from Yahoo Finance."

        cleaned_data = clean_data(raw_data)
        transformed_data = transform_stock_data(cleaned_data)
        if not validate_data(transformed_data):
            return False, "Initial data load failed during validation."

        success = load_data(transformed_data, if_exists="replace")
        if not success:
            return False, "Initial data load failed while writing to the database."

        return True, "Initial dataset loaded successfully."
    except Exception as e:
        return False, f"Initial data load failed: {e}"

# Load DB Data
st.title("📈 Stock Market ETL Pipeline Dashboard")
st.markdown("This dashboard pulls transformed stock data from the local SQLite database.")

df = load_data_from_db()

if df.empty:
    st.warning("No data found in the database.")
    st.info("Run the initial data load once, then the dashboard will become responsive.")
    if st.button("Run Initial Data Load"):
        with st.spinner("Running initial ETL bootstrap..."):
            bootstrap_success, bootstrap_message = run_initial_etl_bootstrap()
        if bootstrap_success:
            st.cache_data.clear()
            st.success(bootstrap_message)
            st.rerun()
        else:
            st.error(bootstrap_message)
else:
    # --- Add New Ticker Section ---
    st.sidebar.header("Add New Stock")
    new_ticker = st.sidebar.text_input("Enter Ticker (e.g., NFLX, META)").upper().strip()
    if st.sidebar.button("Add Ticker"):
        if new_ticker and new_ticker not in df['Ticker'].unique():
            with st.spinner(f"Fetching data for {new_ticker}..."):
                from etl.extract import fetch_stock_data
                from etl.transform import clean_data, transform_stock_data
                from etl.validation import validate_data
                from etl.load import load_data
                
                # Run ETL just for this new ticker
                raw_data = fetch_stock_data([new_ticker], period="1y")
                if not raw_data.empty:
                    cleaned_data = clean_data(raw_data)
                    transformed_data = transform_stock_data(cleaned_data)
                    if validate_data(transformed_data):
                        # Append the new ticker to the database
                        success = load_data(transformed_data, if_exists="append")
                        if success:
                            st.sidebar.success(f"Successfully added {new_ticker}!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.sidebar.error("Failed to load data into database.")
                    else:
                        st.sidebar.error("Data validation failed for this ticker.")
                else:
                    st.sidebar.error(f"Could not fetch data for {new_ticker}. Is the ticker correct?")
        elif new_ticker in df['Ticker'].unique():
            st.sidebar.warning(f"'{new_ticker}' is already in the database.")
            
    st.sidebar.divider()
    
    # --- Sidebar Filters ---
    st.sidebar.header("Filter Options")
    
    # Ticker Filter
    tickers = df['Ticker'].unique().tolist()
    selected_tickers = st.sidebar.multiselect("Select Tickers to View", tickers, default=tickers)
    
    # Date Range Filter
    min_date = df['Date'].min()
    max_date = df['Date'].max()
    date_range = st.sidebar.date_input("Select Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    
    # Filter Data
    if len(date_range) == 2:
        start_date, end_date = date_range
        # Convert date representations to datetime for comparison
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        filtered_df = df[
            (df['Ticker'].isin(selected_tickers)) &
            (df['Date'] >= start_date) &
            (df['Date'] <= end_date)
        ]
    else:
        filtered_df = df[df['Ticker'].isin(selected_tickers)]

    chart_df = _limit_points_per_ticker(filtered_df)
        
    # Main Metrics Summary
    st.subheader("Summary Metrics (Last Available Trading Day)")
    
    if not filtered_df.empty:
        latest_date = filtered_df['Date'].max()
        latest_data = filtered_df[filtered_df['Date'] == latest_date]
        
        cols = st.columns(len(selected_tickers) if len(selected_tickers) > 0 else 1)
        for i, ticker in enumerate(selected_tickers):
            with cols[i % len(cols)]:
                ticker_data = latest_data[latest_data['Ticker'] == ticker]
                if not ticker_data.empty:
                    current_price = ticker_data['Close'].values[0]
                    daily_change = ticker_data['Daily_Return_Pct'].values[0]
                    
                    st.metric(
                        label=f"{ticker} Close",
                        value=f"${current_price:.2f}",
                        delta=f"{daily_change:.2f}%"
                    )
        
        # Line Chart: Closing Prices over time
        st.subheader("Closing Prices Over Time")
        if HAS_PLOTLY:
            fig_close = px.line(chart_df, x='Date', y='Close', color='Ticker', title="Stock Closing Prices")
            st.plotly_chart(fig_close, use_container_width=True)
        else:
            close_chart_df = chart_df.pivot(index='Date', columns='Ticker', values='Close').sort_index()
            st.line_chart(close_chart_df)
        
        # Moving Averages View for a single selected ticker
        st.subheader("Moving Averages Analysis")
        ma_ticker = st.selectbox("Select Ticker for MA Analysis", selected_tickers)
        
        if ma_ticker:
            ma_df = chart_df[chart_df['Ticker'] == ma_ticker]
            if HAS_PLOTLY:
                fig_ma = go.Figure()
                fig_ma.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['Close'], mode='lines', name='Close Price', opacity=0.8))
                fig_ma.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['MA_7_Days'], mode='lines', name='7-Day MA', opacity=0.6))
                fig_ma.add_trace(go.Scatter(x=ma_df['Date'], y=ma_df['MA_30_Days'], mode='lines', name='30-Day MA', opacity=0.6))
                fig_ma.update_layout(title=f"{ma_ticker} - Price vs Moving Averages", xaxis_title="Date", yaxis_title="Price ($)")
                st.plotly_chart(fig_ma, use_container_width=True)
            else:
                ma_chart_df = ma_df.set_index('Date')[['Close', 'MA_7_Days', 'MA_30_Days']].sort_index()
                st.line_chart(ma_chart_df)
            
        # Volatility Bar Chart
        st.subheader("Stock Volatility (30-Day Rolling Std Dev)")
        # Plot volatility over time
        if HAS_PLOTLY:
            fig_vol = px.line(chart_df, x='Date', y='Volatility_30_Days', color='Ticker', title="30-Day Volatility Tracking")
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            vol_chart_df = chart_df.pivot(index='Date', columns='Ticker', values='Volatility_30_Days').sort_index()
            st.line_chart(vol_chart_df)
        
        # View Raw Data Toggle
        if st.checkbox("Show Raw Transformed Data"):
            st.write(filtered_df.sort_values(by=['Date', 'Ticker'], ascending=[False, True]))

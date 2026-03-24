# Stock Market ETL Pipeline

**Live Dashboard:** [View on Streamlit Cloud](https://stock-market-etl-pipeline.streamlit.app)

This project is a complete Extract, Transform, Load (ETL) pipeline that pulls historical stock data from Yahoo Finance, transforms it to add moving averages and volatility scores, and loads it into a SQLite database. The pipeline is scheduled to run automatically, and a Streamlit dashboard visualizes the stock trends.

## Features

*   **Extract:** Pulls historical stock data for Apple, Google, Microsoft, Amazon, and Tesla using `yfinance`.
*   **Transform:**
    *   Cleans null values.
    *   Calculates 7-day and 30-day moving averages.
    *   Calculates daily percentage change.
    *   Adds a volatility score (rolling standard deviation).
*   **Validate:** Checks for missing data, duplicates, and statistical anomalies before loading.
*   **Load:** Saves the transformed data into a SQLite database (`stock_data.db`).
*   **Schedule:** Uses `APScheduler` to run the ETL process automatically.
*   **Visualize:** Provides a Streamlit dashboard to view stock trends interactively.
*   **Logging:** Detailed logging throughout every step of the pipeline for monitoring and debugging.

## Architecture & Components

*   `main.py`: The entry point that orchestrates the ETL process and sets up the scheduler.
*   `etl/extract.py`: Handles fetching data from the Yahoo Finance API.
*   `etl/transform.py`: Performs data cleaning and calculates new metrics (moving averages, volatility, etc.).
*   `etl/validation.py`: Ensures data quality before it reaches the database.
*   `etl/load.py`: Manages the SQLite database connection, schema creation, and data insertion.
*   `utils/logger.py`: Configures the custom logging setup.
*   `dashboard/app.py`: The Streamlit web application for visualizing the data.

## Getting Started

### Prerequisites

*   Python 3.8+

### Installation

1.  Clone this repository or download the source code.
2.  Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

### Running the ETL Pipeline

To execute the ETL pipeline immediately and start the scheduler, run:

```bash
python main.py
```

The pipeline will execute an initial run immediately and then schedule itself to run daily at market close (e.g., 4:30 PM).

### Running the Dashboard

To start the interactive Streamlit dashboard, run the following command in a new terminal window:

```bash
streamlit run dashboard/app.py
```

This will start a local web server (usually at `http://localhost:8501`) where you can view the stock trends.

## Streamlit Cloud Deployment

If deployment fails while installing dependencies, it is usually a Python-version-to-wheel mismatch (for example, when pandas/numpy wheels are not available for the selected runtime).

This project is configured for Streamlit Cloud with:

*   `runtime.txt` set to Python 3.11.11.
*   Pinned versions in `requirements.txt` that provide prebuilt wheels on Linux.

Use these Streamlit Cloud settings:

*   **Main file path:** `dashboard/app.py`
*   **Python version:** automatically read from `runtime.txt`

After deploy:

*   The app uses a local SQLite file by default (`stock_data.db`).
*   On Streamlit Cloud, local files are ephemeral and may reset on restart/redeploy.
*   For persistence, set `DATABASE_URL` to a managed Postgres instance in Streamlit Secrets.

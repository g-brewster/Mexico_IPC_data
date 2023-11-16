import yfinance as yf
import pandas as pd
from pyrate_limiter import Duration, RequestRate, Limiter
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from requests import Session
import datetime

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

# Setting up the session with rate limiting and caching
session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND * 5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)
session.headers['User-agent'] = 'Yahoo-F_Api'

# Load the tickers
emisoras = pd.read_excel("data/emisoras.xls")
tickers_list = emisoras["Symbol"]

# Prepare to store the historical data
historical_data = {}

# Define the date range for the past 10 years
end_date = datetime.datetime.today()
start_date = end_date - datetime.timedelta(days=365*10)

# Function to fetch data
def fetch_data(ticker_symbol):
    try:
        yf_ticker = yf.Ticker(ticker_symbol, session=session)
        return yf_ticker.history(start=start_date, end=end_date)
    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return None

# Retrieve data for each ticker
for ticker in tickers_list:
    data = fetch_data(ticker)
    if data is not None:
        historical_data[ticker] = data

# Concatenate all DataFrames with an additional column for the ticker symbol
all_data = pd.concat(
    [df.assign(Ticker=ticker) for ticker, df in historical_data.items()],
    ignore_index=True
)

# Write the combined data to a CSV file
all_data.to_csv("data/historical_data.csv", index=False)
print("Data written to historical_data.csv")

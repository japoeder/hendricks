"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
import pytz  # Add this import
import pandas as pd
import json
from dotenv import load_dotenv
load_dotenv()
from alpaca_trade_api import REST
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists


def load_historical_quote_alpacaAPI(
    ticker_symbol,
    collection_name,
    from_date,
    to_date,
    creds_file_path="/home/jonathan/pydev/quantum_trade/_cred/creds.json",
    batch_size=7500,
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    # Load Alpaca API credentials from JSON file
    API_KEY, API_SECRET, BASE_URL = load_credentials(creds_file_path)

    # Initialize the Alpaca API
    api = REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

    # Run time conversion
    TZ = pytz.timezone('America/New_York')

    # Create a timezone-aware timestamp in America/New_York
    from_date = pd.Timestamp(from_date, tz=TZ)
    from_date = from_date.tz_convert('UTC').isoformat()

    to_date = pd.Timestamp(to_date, tz=TZ)
    to_date = to_date.tz_convert('UTC').isoformat()

    # Fetch the data
    barset = api.get_bars(ticker_symbol, "1Min", start=from_date, end=to_date).df

    # Prepare the DataFrame
    barset.reset_index(inplace=True)
    barset.columns = barset.columns.str.lower()
    barset["ticker"] = ticker_symbol

    # Get the database connection
    db = mongo_conn()

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Prepare documents for batch insert
    documents = []
    est = pytz.timezone('US/Eastern')  # Define the EST timezone

    # Save barset to a file
    barset.to_csv('/home/jonathan/pydev/quantum_trade/hendricks/_data/barset.csv', index=False)

    for _, row in barset.iterrows():
        timestamp_utc = row["timestamp"]
        #print(f'timestamp_utc: {timestamp_utc}' )
        # timestamp_est = timestamp_utc.tz_convert(TZ)
        # print(f'timestamp_est: {timestamp_est}')
        document = {
            "ticker": row["ticker"],
            "timestamp": timestamp_utc,
            "open": row["open"],
            "low": row["low"],
            "high": row["high"],
            "close": row["close"],
            "volume": row["volume"],
            "trade_count": row.get("trade_count", 0),  # Default to 0 if not present
            "vwap": row.get("vwap", 0),  # Default to 0 if not present
            "created_at": datetime.now(timezone.utc),  # Document creation time in UTC
        }
        documents.append(document)

        # Insert in batches
        if len(documents) >= batch_size:
            collection.insert_many(documents)
            documents = []

    #print(f'documents: {documents}')
    # Insert any remaining documents
    if documents:
        collection.insert_many(documents)

    print("Data imported successfully!")



# Example usage
# load_historical_quote_alpacaAPI(ticker_symbol='GOOG',
#                                 collection_name='rawPriceColl',
#                                 from_date='2024-11-01T00:00:00',
#                                 to_date='2024-11-02T23:59:00')
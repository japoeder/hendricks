"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
from alpaca_trade_api import REST
from _utils.mongo_conn import mango_conn
from _utils.mongo_coll_verification import confirm_mongo_collect_exists
import json


def load_credentials(file_path):
    """
    Load Alpaca API credentials from JSON file.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        creds = json.load(file)
    alpaca_creds = creds["alpaca_api"]
    return (
        alpaca_creds["API_KEY"],
        alpaca_creds["API_SECRET"],
        alpaca_creds["PAPER_URL"],
    )


def load_historical_quote_alpacaAPI(
    ticker_symbol,
    collection_name,
    start_date,
    end_date,
    creds_file_path="/Volumes/documents/pydev/quantum_trade/_cred/creds.json",
    batch_size=7500,
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    # Load Alpaca API credentials from JSON file
    API_KEY, API_SECRET, BASE_URL = load_credentials(creds_file_path)

    # Initialize the Alpaca API
    api = REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

    # Fetch the data
    barset = api.get_bars(ticker_symbol, "1Min", start=start_date, end=end_date).df

    # Prepare the DataFrame
    barset.reset_index(inplace=True)
    barset.columns = barset.columns.str.lower()
    barset["ticker"] = ticker_symbol

    # Get the database connection
    db = mango_conn()

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Prepare documents for batch insert
    documents = []
    for _, row in barset.iterrows():
        document = {
            "ticker": row["ticker"],
            "timestamp": row["timestamp"],
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

    # Insert any remaining documents
    if documents:
        collection.insert_many(documents)

    print("Data imported successfully!")


# Example usage
# load_historical_quote_alpacaAPI('GOOG'
#                                 , 'historicalPrices'
#                                 , '2018-01-01T09:30:00-04:00'
#                                 , datetime.now().strftime('%Y-%m-%dT%H:%M:%S-04:00')
#                                 )

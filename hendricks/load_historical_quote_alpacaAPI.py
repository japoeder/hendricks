"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone, timedelta
import pytz  # Add this import
import pandas as pd
from dateutil.relativedelta import relativedelta
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
    creds_file_path="/home/japoeder/pydev/quantum_trade/_cred/creds.json",
    batch_size=7500
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

    # Get the database connection
    db = mongo_conn()

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]
    backup_collection = db[f"{collection_name}_bak"]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)
    collection.create_index([("timestamp", 1), ("ticker", 1), ("archived_at")], unique=True)

    current_date = from_date
    while current_date <= to_date:
        # Calculate the end of the current month
        month_end = (current_date + relativedelta(months=1)) - timedelta(seconds=1)
        if month_end > to_date:
            month_end = to_date

        # Fetch the data for the current month
        barset = api.get_bars(ticker_symbol, "1Min", start=current_date.isoformat(), end=month_end.isoformat()).df

        # Prepare the DataFrame
        barset.reset_index(inplace=True)
        barset.columns = barset.columns.str.lower()
        barset["ticker"] = ticker_symbol

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

            #print(f'document: {document}')
            # Check if the document exists
            existing_doc = collection.find_one({"timestamp": row["timestamp"], "ticker": row["ticker"]})
            if existing_doc:
                # Compare the existing document with the new data
                fields_to_check = ["open", "low", "high", "close", "volume", "trade_count", "vwap"]
                if any(existing_doc[field] != document[field] for field in fields_to_check):
                    # Backup the existing document
                    existing_doc["archived_at"] = datetime.now(timezone.utc)
                    backup_collection.insert_one(existing_doc)

                # Upsert logic
                collection.update_one(
                    {"timestamp": row["timestamp"], "ticker": row["ticker"]},  # Query
                    {"$set": document},  # Update
                    upsert=True  # Upsert option
                )
            else:
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
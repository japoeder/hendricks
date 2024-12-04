"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
import pytz
import pandas as pd
from dotenv import load_dotenv
from alpaca_trade_api import REST

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path


def quote_from_alpacaAPI(
    tickers=None,
    collection_name="rawPriceColl",
    creds_file_path=None,
    from_date=None,
    to_date=None,
    batch_size=75000,
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, API_SECRET, BASE_URL = load_credentials(creds_file_path)

    # Initialize the Alpaca API
    api = REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

    # Run time conversion
    TZ = pytz.timezone("America/New_York")

    # Convert from_date and to_date to timezone-aware datetime objects
    from_date = pd.Timestamp(from_date, tz=TZ).to_pydatetime()
    to_date = pd.Timestamp(to_date, tz=TZ).to_pydatetime()

    # Get the database connection
    db = mongo_conn()

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Fetch the data for the entire date range
    try:
        barset = api.get_bars(
            tickers, "1Min", start=from_date.isoformat(), end=to_date.isoformat()
        ).df
    except Exception as e:
        print(f"Error fetching data from Alpaca API: {e}")
        return

    # Prepare the DataFrame
    barset.reset_index(inplace=True)
    barset.columns = barset.columns.str.lower()

    # Rename barset 'symbol' to 'ticker'
    barset.rename(columns={"symbol": "ticker"}, inplace=True)

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

        # Check if the document exists
        existing_doc = collection.find_one(
            {"timestamp": row["timestamp"], "ticker": row["ticker"]}
        )
        if existing_doc:
            # Compare fields except for 'created_at'
            if (
                existing_doc["open"] == document["open"]
                and existing_doc["low"] == document["low"]
                and existing_doc["high"] == document["high"]
                and existing_doc["close"] == document["close"]
                and existing_doc["volume"] == document["volume"]
                and existing_doc["trade_count"] == document["trade_count"]
                and existing_doc["vwap"] == document["vwap"]
            ):
                # Fields are the same, do nothing
                continue
            else:  # pylint: disable=no-else-return
                # Upsert logic if fields are different
                collection.update_one(
                    {"timestamp": row["timestamp"], "ticker": row["ticker"]},  # Query
                    {"$set": document},  # Update
                    upsert=True,  # Upsert option
                )
        else:
            # Insert the document directly
            collection.insert_one(document)

    print("Data imported successfully!")

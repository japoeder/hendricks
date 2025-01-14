"""
Load historical quote data from FMP API into a MongoDB collection.
"""

from datetime import datetime, timezone
import logging
import pytz
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path
from hendricks._utils.request_url_constructor import request_url_constructor
from hendricks._utils.exceptions import APIError

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def quote_from_fmpAPI(
    tickers=None,
    collection_name=None,
    creds_file_path=None,
    from_date=None,
    to_date=None,
    mongo_db="StocksDB",
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, "fmp_api_findata")

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

    # Convert from_date and to_date to 'yyyy-mm-dd' format
    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    for ticker in tickers:
        url = request_url_constructor(
            endpoint="historical-chart",
            base_url=BASE_URL,
            interval="1min",
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            api_key=API_KEY,
            extended="true",
            source="fmp",
        )

        response = requests.get(url)

        if response.status_code == 200:
            quotes = response.json()

            # Skip processing if no data returned
            if not quotes:
                logger.info(
                    f"No data returned for {ticker} between {from_date} and {to_date}"
                )
                continue

            # Process quotes to rename 'date' to 'timestamp'
            for quote in quotes:
                quote["timestamp"] = quote.pop("date")  # Rename 'date' to 'timestamp'

        else:
            raise APIError(f"Error: {response.status_code}, {response.text}")

        # Sort results by timestamp in descending order
        quotes_df = pd.DataFrame(quotes)
        quotes_df = quotes_df.sort_values(by="timestamp", ascending=False)

        # Process quotes in bulk instead of one by one
        bulk_operations = []
        for quote in quotes_df.iterrows():
            quote = quote[1]

            # Convert timestamp to UTC timezone
            quote["timestamp"] = pd.Timestamp(
                quote["timestamp"], tz="America/New_York"
            ).tz_convert("UTC")

            document = {
                "ticker": ticker,
                "timestamp": quote["timestamp"],
                "open": quote["open"],
                "low": quote["low"],
                "high": quote["high"],
                "close": quote["close"],
                "volume": quote["volume"],
                "source": "fmp",
                "created_at": datetime.now(timezone.utc),
            }

            # Create update operation that only updates if values are different
            bulk_operations.append(
                UpdateOne(
                    {
                        "timestamp": document["timestamp"],
                        "ticker": document["ticker"],
                        # Only update if any of these values are different
                        "$or": [
                            {"open": {"$ne": document["open"]}},
                            {"low": {"$ne": document["low"]}},
                            {"high": {"$ne": document["high"]}},
                            {"close": {"$ne": document["close"]}},
                            {"volume": {"$ne": document["volume"]}},
                        ],
                    },
                    {"$set": document},
                    upsert=True,
                )
            )

        # Execute all bulk operations at once
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                logger.info(f"Processed {len(bulk_operations)} quotes for {ticker}")
                logger.info(
                    f"Inserted: {result.upserted_count}, Modified: {result.modified_count}"
                )
            except BulkWriteError as bwe:
                # Only log the count of failed operations instead of full details
                failed_count = len(bwe.details.get("writeErrors", []))
                logger.warning(f"{failed_count} duplicate entries skipped for {ticker}")

        print(f"Data import completed for {ticker}")

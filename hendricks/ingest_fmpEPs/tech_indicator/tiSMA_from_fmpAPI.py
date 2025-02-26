"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

# from datetime import timezone
import logging

# import pytz
import hashlib
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

# from gridfs import GridFS

load_dotenv()
from quantum_trade_utilities.data.load_credentials import load_credentials
from quantum_trade_utilities.data.mongo_conn import mongo_conn
from quantum_trade_utilities.data.mongo_coll_verification import (
    confirm_mongo_collect_exists,
)
from quantum_trade_utilities.core.get_path import get_path
from quantum_trade_utilities.data.request_url_constructor import request_url_constructor

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def tiSMA_from_fmpAPI(
    tickers=None,
    collection_name=None,
    creds_file_path=None,
    from_date=None,
    to_date=None,
    ep=None,
    freq="1min",
    freq_range=5,
    mongo_db="stocksDB",
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    ep_timestamp_field = "date"
    cred_key = "fmp_api_findata"

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, cred_key)

    # Get the database connection
    db = mongo_conn(mongo_db=mongo_db)

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name, mongo_db)

    # Get the collection
    collection = db[collection_name]

    # Create indexes for common query patterns
    collection.create_index([("timestamp", 1)])  # For date range queries
    collection.create_index([("ticker", 1)])  # For ticker queries
    collection.create_index([("unique_id", 1)])  # For source filtering

    collection.create_index(
        [("ticker", 1), ("timestamp", -1)]
    )  # For ticker + time sorting
    collection.create_index(
        [("unique_id", 1), ("timestamp", -1)]
    )  # For source + time sorting

    # Uniqueness constraint
    collection.create_index(
        [("unique_id", 1), ("ticker", 1)],
        unique=True,
        background=True,  # Allow other operations while building index
    )

    for ticker in tickers:
        url = request_url_constructor(
            endpoint=ep,
            base_url=BASE_URL,
            ticker=ticker,
            api_key=API_KEY,
            source="fmp",
            from_date=from_date,
            to_date=to_date,
            freq=freq,
            freq_range=freq_range,
        )

        print(f"URL: {url}")

        # Get the news data
        response = requests.get(url)
        logger.info(f"FMP API URL: {url}")
        logger.info(f"FMP API Response Status: {response.status_code}")

        if response.status_code == 200:
            res = response.json()
            logger.info(f"FMP API Response Length: {len(res)}")

            if len(res) == 0:
                logger.info(f"No data found for {ticker}")
                continue

            # Convert news list of dictionaries to a pandas DataFrame
            res_df = pd.DataFrame(res)
            logger.info(f"DataFrame shape: {res_df.shape}")
            logger.info(f"DataFrame columns: {res_df.columns.tolist()}")

            # Sort results by timestamp in descending order
            if ep_timestamp_field != "today":
                res_df.sort_values(by=ep_timestamp_field, ascending=False, inplace=True)

            # Process news items in bulk
            bulk_operations = []
            for _, row in res_df.iterrows():
                if ep_timestamp_field == "today":
                    timestamp = datetime.now(ZoneInfo("America/Chicago"))
                elif ep_timestamp_field == "year":
                    # Jan 1st of the year
                    timestamp = datetime(
                        int(row["year"]), 1, 1, tzinfo=ZoneInfo("America/New_York")
                    )
                elif ep_timestamp_field == "timestamp":
                    timestamp = datetime.fromtimestamp(
                        row["timestamp"], tz=ZoneInfo("America/New_York")
                    )
                else:
                    raw_date = row[ep_timestamp_field]
                    if (
                        isinstance(raw_date, str) and len(raw_date.split()) == 1
                    ):  # Just a date
                        # Parse the date and explicitly set to midnight EST
                        date_obj = datetime.strptime(raw_date, "%Y-%m-%d").date()
                        timestamp = datetime.combine(
                            date_obj,
                            datetime.min.time(),
                            tzinfo=ZoneInfo("America/New_York"),  # Explicitly EST
                        )
                    else:  # Has time component
                        # Parse with pandas and ensure EST
                        timestamp = pd.to_datetime(raw_date)
                        if timestamp.tzinfo is None:
                            # If no timezone provided, add EST to the datetime object
                            timestamp = timestamp.tz_localize("America/New_York")
                        else:
                            # If it has a timezone, convert to EST
                            timestamp = timestamp.astimezone(
                                ZoneInfo("America/New_York")
                            )

                created_at = datetime.now(ZoneInfo("America/Chicago"))

                # Create a hash of the actual estimate values to detect changes
                feature_values = {
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "sma": row["sma"],
                }
                feature_hash = hashlib.sha256(str(feature_values).encode()).hexdigest()

                # Create unique_id when there isn't a good option in response
                f1 = ticker
                f2 = timestamp

                # Create hash of f1, f2
                unique_id = hashlib.sha256(f"{f1}{f2}".encode()).hexdigest()

                # Streamlined main document
                document = {
                    "unique_id": unique_id,
                    "timestamp": timestamp,
                    "ticker": ticker,
                    ##########################################
                    ##########################################
                    "date": row["date"],
                    # Unpack the feature_hash
                    **feature_values,
                    "feature_hash": feature_hash,
                    ##########################################
                    ##########################################
                    "source": "fmp",
                    "created_at": created_at,
                }

                # Replace the find_one and separate insert/update with a single upsert
                bulk_operations.append(
                    UpdateOne(
                        {
                            "date": document["date"],
                            # Only update if hash is different or document doesn't exist
                            "$or": [
                                {"feature_hash": {"$ne": feature_hash}},
                                {"feature_hash": {"$exists": False}},
                            ],
                        },
                        {"$set": document},
                        upsert=True,
                    )
                )

            if bulk_operations:
                try:
                    result = collection.bulk_write(bulk_operations, ordered=False)
                    logger.info(
                        f"Processed {len(bulk_operations)} new items for {ticker}"
                    )
                    logger.info(
                        f"Inserted: {result.upserted_count}, Modified: {result.modified_count}"
                    )
                except BulkWriteError as bwe:
                    # Filter out duplicate key errors (code 11000)
                    non_duplicate_errors = [
                        error
                        for error in bwe.details["writeErrors"]
                        if error["code"] != 11000
                    ]

                    # Only log if there are non-duplicate errors
                    if non_duplicate_errors:
                        logger.warning(
                            f"Some writes failed for {ticker}: {non_duplicate_errors}"
                        )

            logger.info("Data imported successfully!")

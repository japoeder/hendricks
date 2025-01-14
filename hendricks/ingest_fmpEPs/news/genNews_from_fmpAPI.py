"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime
import hashlib
import logging
from zoneinfo import ZoneInfo

# import pytz
import time
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

# from gridfs import GridFS

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path
from hendricks._utils.request_url_constructor import request_url_constructor

# from hendricks._utils.grab_html import grab_html

# from hendricks._utils.std_article_time import std_article_time

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def genNews_from_fmpAPI(
    tickers=None,
    collection_name=None,
    creds_file_path=None,
    from_date=None,
    to_date=None,
    ep: str = None,
    mongo_db="stocksDB",
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    ep_timestamp_field = "publishedDate"
    cred_key = "fmp_api_findata_v4"

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, cred_key)

    # Convert from_date and to_date to timezone-aware datetime objects
    # Switch from UTC to EST using tz_convert
    from_date = from_date.tz_convert("America/New_York")
    to_date = to_date.tz_convert("America/New_York")

    # Get the database connection
    db = mongo_conn(mongo_db=mongo_db)

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create indexes for common query patterns
    collection.create_index([("timestamp", 1)])  # For date range queries
    collection.create_index([("ticker", 1)])  # For ticker queries
    collection.create_index([("source", 1)])  # For source filtering

    # Compound indexes for common query combinations
    collection.create_index(
        [("ticker", 1), ("timestamp", -1)]
    )  # For ticker + time sorting
    collection.create_index(
        [("source", 1), ("timestamp", -1)]
    )  # For source + time sorting

    # Uniqueness constraint
    collection.create_index(
        [("unique_id", 1), ("ticker", 1)],
        unique=True,
        background=True,  # Allow other operations while building index
    )

    # Convert from_date and to_date to 'yyyy-mm-dd' format
    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    for ticker in tickers:
        page = 0
        has_more_data = True

        while has_more_data:
            url = request_url_constructor(
                endpoint="general_news",
                base_url=BASE_URL,
                from_date=from_date,
                to_date=to_date,
                api_key=API_KEY,
                source="fmp",
                page=page,
            )

            print(f"URL: {url}")
            response = requests.get(url)

            if response.status_code != 200:
                logger.warning(f"Failed to fetch page {page} for {ticker}")
                break

            res = response.json()

            # Check if we got any data back
            if not res or len(res) == 0:
                logger.info(f"No more data for {ticker} after page {page}")
                has_more_data = False
                break

            # Process the page data
            res_df = pd.DataFrame(res)
            logger.info(
                f"Processing page {page} with {len(res_df)} records for {ticker}"
            )

            # Sort results by timestamp in descending order
            if ep_timestamp_field != "today":
                res_df.sort_values(by=ep_timestamp_field, ascending=False, inplace=True)

            # Process news items in bulk
            bulk_operations = []
            for _, row in res_df.iterrows():
                # html_content = grab_html(row["url"])

                cleaned_date = row["publishedDate"].replace("T", " ").split(".")[0]
                # Parse with pandas
                timestamp = pd.to_datetime(cleaned_date, utc=True)
                # Convert to EST
                timestamp = timestamp.tz_convert("America/New_York")

                created_at = datetime.now(ZoneInfo("America/Chicago"))

                # Create a hash of the actual estimate values to detect changes
                feature_values = {
                    "headline": row["title"],
                    "link": row["url"],
                }
                feature_hash = hashlib.sha256(str(feature_values).encode()).hexdigest()

                # Create unique_id when there isn't a good option in response
                f1 = row["publishedDate"]
                f2 = row["title"]
                f3 = row["text"]
                f4 = row["url"]

                # Create hash of f1, f2, f3, f4
                unique_id = hashlib.sha256(f"{f1}{f2}{f3}{f4}".encode()).hexdigest()

                # Streamlined main document
                document = {
                    "unique_id": unique_id,
                    "timestamp": timestamp,
                    "ticker": ticker,
                    ##########################################
                    ##########################################
                    "site": row["site"],
                    "publishedDate": row["publishedDate"],
                    "text": row["text"],
                    "image": row["image"],
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
                            # Create unique_id when there isn't a good option in response
                            "timestamp": document["timestamp"],
                            "link": document["link"],
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

            # Execute bulk operations for this page
            if bulk_operations:
                try:
                    result = collection.bulk_write(bulk_operations, ordered=False)
                    logger.info(
                        f"Page {page}: Processed {len(bulk_operations)} items for {ticker}"
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
                finally:
                    bulk_operations = []  # Clear bulk operations for next page

            page += 1

            # Optional: Add a small delay to avoid hitting rate limits
            time.sleep(0.5)

        logger.info(f"Completed processing for {ticker}")

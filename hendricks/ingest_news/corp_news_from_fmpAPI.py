"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime
import hashlib
import logging
from zoneinfo import ZoneInfo
import pytz
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from gridfs import GridFS

load_dotenv()
from quantum_trade_utilities.data.load_credentials import load_credentials
from quantum_trade_utilities.data.mongo_conn import mongo_conn
from quantum_trade_utilities.data.mongo_coll_verification import (
    confirm_mongo_collect_exists,
)
from quantum_trade_utilities.core.get_path import get_path
from quantum_trade_utilities.data.request_url_constructor import request_url_constructor
from quantum_trade_utilities.io.grab_html import grab_html

# from hendricks._utils.std_article_time import std_article_time

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def news_from_fmpAPI(
    tickers=None,
    collection_name=None,
    gridfs_bucket=None,
    creds_file_path=None,
    from_date=None,
    to_date=None,
    articles_limit: int = 1,
    include_content: bool = True,
    mongo_db: str = "stocksDB",
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    ep_timestamp_field = "publishedDate"
    cred_key = "fmp_api_findata"

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, cred_key)

    # Run time conversion
    TZ = pytz.timezone("America/New_York")

    # Convert from_date and to_date to timezone-aware datetime objects
    from_date = pd.Timestamp(from_date, tz=TZ).to_pydatetime()
    to_date = pd.Timestamp(to_date, tz=TZ).to_pydatetime()

    # Get the database connection
    db = mongo_conn(mongo_db=mongo_db)

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name, mongo_db)

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

    # Initialize GridFS
    fs = GridFS(db, collection=gridfs_bucket)

    for ticker in tickers:
        page = 0
        while True:  # Replace a=True with clearer logic
            url = request_url_constructor(
                endpoint="stock_news",
                base_url=BASE_URL,
                ticker=ticker,
                from_date=from_date,
                to_date=to_date,
                api_key=API_KEY,
                source="fmp",
                page=page,
            )

            print(f"URL: {url}")
            response = requests.get(url)

            if response.status_code != 200:
                # logger.warning(f"Failed to fetch page {page} for {ticker}")
                break

            res = response.json()
            if not res:  # No more data
                logger.info(f"No more data for {ticker} after page {page}")
                break

            # Process the page data
            res_df = pd.DataFrame(res)
            logger.info(f"DataFrame shape: {res_df.shape}")
            logger.info(f"DataFrame columns: {res_df.columns.tolist()}")

            # Sort results by timestamp in descending order
            if ep_timestamp_field != "today":
                res_df.sort_values(by=ep_timestamp_field, ascending=False, inplace=True)

            # Sort results by publishedDate in descending order
            res_df.sort_values(by="publishedDate", ascending=False, inplace=True)

            # Process news items in bulk
            bulk_operations = []
            for _, row in res_df.iterrows():
                html_content = grab_html(row["url"])

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
                    "headline": row["title"],
                    "link": row["url"],
                }
                feature_hash = hashlib.sha256(str(feature_values).encode()).hexdigest()

                # Create unique_id when there isn't a good option in response
                f1 = ticker
                f2 = row["publishedDate"]
                f3 = row["title"]
                f4 = row["text"]
                f5 = row["url"]

                # Create hash of f1, f2, f3, f4
                unique_id = hashlib.sha256(f"{f1}{f2}{f3}{f4}{f5}".encode()).hexdigest()

                # Store large content in GridFS
                content_data = {
                    "summary": row["text"],
                    "content": row.get("content", "N/A"),
                    "images": row.get("image"),
                    "html": html_content,
                    "article_tickers": [row["symbol"]],
                    "author": "N/A",
                    "article_created_at": row["publishedDate"],
                    "article_updated_at": row["publishedDate"],
                    # "timestamp_conversion_result": conversion_result[1],
                }

                # Store in GridFS with metadata
                content_id = fs.put(
                    str(content_data).encode("utf-8"),
                    filename=row["url"],
                    ticker=ticker,
                    source="fmp",
                )

                # Streamlined main document
                document = {
                    "unique_id": unique_id,
                    "timestamp": timestamp,
                    "ticker": ticker,
                    ##########################################
                    ##########################################
                    "article_source": row["site"],
                    # Unpack the feature_hash
                    **feature_values,
                    "feature_hash": feature_hash,
                    ##########################################
                    ##########################################
                    "source": "fmp",
                    "created_at": created_at,
                    "content_id": content_id,  # Reference to GridFS content
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

            # Check if we've hit the articles limit
            if articles_limit and page * len(res) >= articles_limit:
                logger.info(f"Reached articles limit of {articles_limit} for {ticker}")
                break

        logger.info(f"Completed processing for {ticker}")

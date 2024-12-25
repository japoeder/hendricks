"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
import logging
import pytz
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from gridfs import GridFS

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path
from hendricks._utils.request_url_constructor import request_url_constructor
from hendricks._utils.grab_html import grab_html
from hendricks._utils.std_article_time import std_article_time

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def news_from_fmpAPI(
    tickers=None,
    collection_name="rawNewsColl",
    creds_file_path=None,
    from_date=None,
    to_date=None,
    articles_limit: int = 1,
    include_content: bool = True,
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, "fmp_api")

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
    fs = GridFS(db)

    for ticker in tickers:
        a = True
        page = 0
        while a:
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

            # Get the news data
            response = requests.get(url)
            logger.info(f"FMP API URL: {url}")
            logger.info(f"FMP API Response Status: {response.status_code}")

            if response.status_code == 200:
                news = response.json()
                logger.info(f"FMP API Response Length: {len(news)}")

                if len(news) == 0:
                    a = False
                    if page == 0:
                        logger.info(f"No articles found for {ticker}")
                    else:
                        logger.info("Articles imported successfully!")
                    break

                # Convert news list of dictionaries to a pandas DataFrame
                news = pd.DataFrame(news)
                logger.info(f"DataFrame shape: {news.shape}")
                logger.info(f"DataFrame columns: {news.columns.tolist()}")

                # Rename barset 'symbol' to 'ticker'
                news.rename(columns={"symbol": "ticker"}, inplace=True)

                # Sort results by publishedDate in descending order
                news.sort_values(by="publishedDate", ascending=False, inplace=True)

                # Process news items in bulk
                bulk_operations = []
                for _, row in news.iterrows():
                    html_content = grab_html(row["url"])

                    # Convert the publishedDate to UTC
                    conversion_result = std_article_time(
                        row["publishedDate"], row["site"]
                    )
                    if conversion_result[1] == "converted":
                        publishedDate = conversion_result[0]
                    else:
                        publishedDate = row["publishedDate"]

                    # Store large content in GridFS
                    content_data = {
                        "summary": row["text"],
                        "content": row.get("content", "N/A"),
                        "images": row.get("image"),
                        "html": html_content,
                        "article_tickers": [row["ticker"]],
                        "author": "N/A",
                        "article_created_at": publishedDate,
                        "article_updated_at": publishedDate,
                        "timestamp_conversion_result": conversion_result[1],
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
                        "unique_id": row["url"],
                        "timestamp": publishedDate,
                        "ticker": row["ticker"],
                        "article_source": row["site"],
                        "headline": row["title"],
                        "source": "fmp",
                        "created_at": datetime.now(timezone.utc),
                        "content_id": content_id,  # Reference to GridFS content
                    }

                    # Create update operation
                    bulk_operations.append(
                        UpdateOne(
                            {
                                "unique_id": document["unique_id"],
                                "ticker": document["ticker"],
                            },
                            {"$set": document},
                            upsert=True,
                        )
                    )

                # Execute bulk operations if any exist
                if bulk_operations:
                    try:
                        result = collection.bulk_write(bulk_operations, ordered=False)
                        logger.info(
                            f"Processed {len(bulk_operations)} news items for {ticker}"
                        )
                        logger.info(
                            f"Inserted: {result.upserted_count}, Modified: {result.modified_count}"
                        )
                    except BulkWriteError as bwe:
                        logger.warning(
                            f"Some writes failed for {ticker}: {bwe.details}"
                        )

            page += 1

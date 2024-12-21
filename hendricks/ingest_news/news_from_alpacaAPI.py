"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
import logging
import pytz
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import NewsClient
from alpaca.data.requests import NewsRequest

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path
from hendricks._utils.grab_html import grab_html


# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def news_from_alpacaAPI(
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
    API_KEY, API_SECRET = load_credentials(creds_file_path, "alpaca_news")

    # Initialize the NewsClient (no keys required for news data)
    client = NewsClient(api_key=API_KEY, secret_key=API_SECRET)

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
    collection.create_index(
        [("unique_id", 1), ("timestamp", 1), ("ticker", 1)], unique=True
    )

    for ticker in tickers:
        # drop news variable if it exists
        if "news" in locals():
            del news

        # Create the news request
        request_params = NewsRequest(
            symbols=ticker,
            start=from_date,
            end=to_date,
            limit=articles_limit,
            include_content=include_content,
            sort="DESC",  # Get newest articles first
        )

        # Get the news data
        news = client.get_news(request_params)
        news = news.df

        # Prepare the DataFrame
        news.reset_index(inplace=True)
        news.columns = news.columns.str.lower()

        # Rename barset 'symbol' to 'ticker'
        news.rename(columns={"symbols": "tickers"}, inplace=True)

        # Sort results by publishedDate in descending order
        news.sort_values(by="created_at", ascending=False, inplace=True)

        for _, row in news.iterrows():
            html_content = grab_html(row["url"])
            timestamp = (
                row["created_at"]
                .floor("min")
                .astimezone(pytz.utc)
                .strftime("%Y-%m-%d %H:%M:%S")
            )

            document = {
                "unique_id": row["url"],
                # Convert timestamp to datetime, floor to the nearest minute, convert to UTC, and convert to string
                "timestamp": timestamp,
                "timestamp_conversion_result": "N/A",
                "ticker": ticker,
                "article_id": row["id"],
                "headline": row["headline"],
                "article_source": row["source"],
                "url": row["url"],
                "summary": row["summary"],
                "article_created_at": row["created_at"],
                "article_updated_at": row["updated_at"],
                "article_tickers": row["tickers"],
                "author": row["author"],
                "content": row["content"],
                "images": row["images"],
                "html": html_content,
                "source": "alpaca",
                "created_at": datetime.now(
                    timezone.utc
                ),  # Document creation time in UTC
            }

            # Check if the document exists
            existing_doc = collection.find_one(
                {
                    "unique_id": row["url"],
                    "timestamp": timestamp,
                    "ticker": ticker,
                }
            )
            if not existing_doc:
                # Insert the document directly
                collection.insert_one(document)
                logger.info(f"Inserted document for {ticker} at {row['created_at']}")

    logger.info("Articles imported successfully!")

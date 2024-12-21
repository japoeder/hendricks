"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone, timedelta
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

    print(f"Now executing news_from_alpacaAPI for {tickers}")

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    print("getting credentials")
    API_KEY, API_SECRET = load_credentials(creds_file_path, "alpaca_news")

    # Initialize the NewsClient (no keys required for news data)
    client = NewsClient(api_key=API_KEY, secret_key=API_SECRET)

    print("getting timezone")
    # Run time conversion
    TZ = pytz.timezone("America/New_York")

    print("converting from_date and to_date")
    # Convert from_date and to_date to timezone-aware datetime objects
    if isinstance(from_date, (datetime, pd.Timestamp)) and from_date.tzinfo is not None:
        from_date = pd.Timestamp(from_date).tz_convert(TZ)
    else:
        from_date = pd.Timestamp(from_date, tz=TZ)

    # Calculate to_date as current time minus 15 minutes
    current_time = datetime.now(TZ) - timedelta(minutes=15)
    if to_date is None:
        to_date = pd.Timestamp(current_time)
    elif isinstance(to_date, (datetime, pd.Timestamp)) and to_date.tzinfo is not None:
        to_date = pd.Timestamp(to_date).tz_convert(TZ)
    else:
        to_date = pd.Timestamp(to_date, tz=TZ)

    # Format from_date and to_date like "2024-11-01T00:00:00Z"
    from_date = from_date.strftime("%Y-%m-%d %H:%M:%S")
    to_date = to_date.strftime("%Y-%m-%d %H:%M:%S")

    print("getting database connection")
    # Get the database connection
    db = mongo_conn()

    print("confirming collection exists")
    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    print("getting collection")
    # Get the collection
    collection = db[collection_name]

    print("creating compound index")
    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index(
        [("unique_id", 1), ("timestamp", 1), ("ticker", 1)], unique=True
    )

    print("looping through tickers")
    for ticker in tickers:
        # drop news variable if it exists
        if "news" in locals():
            del news

        print(
            f"from_date: {from_date}, to_date: {to_date}, articles_limit: {articles_limit}, include_content: {include_content}"
        )
        print("creating news request")
        # Create the news request
        request_params = NewsRequest(
            symbols=ticker,
            start=from_date,
            end=to_date,
            limit=articles_limit,
            include_content=include_content,
            sort="DESC",  # Get newest articles first
        )

        print("getting news data")
        # Get the news data
        news = client.get_news(request_params)
        news = news.df

        print("resetting index")
        # Prepare the DataFrame
        news.reset_index(inplace=True)
        news_df = news.copy()
        news_df.columns = news_df.columns.str.lower()

        print("renaming barset 'symbol' to 'ticker'")
        # Rename barset 'symbol' to 'ticker'
        news_df.rename(columns={"symbols": "tickers"}, inplace=True)

        print("looping through rows")
        for _, row in news_df.iterrows():
            print("grabbing html")
            html_content = grab_html(row["url"])

            print("creating timestamp")
            timestamp = (
                row["created_at"]
                .floor("min")
                .astimezone(pytz.utc)
                .strftime("%Y-%m-%d %H:%M:%S")
            )

            print("creating document")
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

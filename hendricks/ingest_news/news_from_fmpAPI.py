"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone
import logging
import pytz
import pandas as pd
from dotenv import load_dotenv
import requests

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

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index(
        [("unique_id", 1), ("timestamp", 1), ("ticker", 1)], unique=True
    )

    # Convert from_date and to_date to 'yyyy-mm-dd' format
    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

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

            if response.status_code == 200:
                news = response.json()

                if len(news) == 0:
                    a = False
                    if page == 0:
                        logger.info(f"No articles found for {ticker}")
                    else:
                        logger.info("Articles imported successfully!")
                    break

            # Convert news list of dictionaries to a pandas DataFrame
            news = pd.DataFrame(news)

            # Rename barset 'symbol' to 'ticker'
            news.rename(columns={"symbol": "ticker"}, inplace=True)

            # Sort results by publishedDate in descending order
            news.sort_values(by="publishedDate", ascending=False, inplace=True)

            for _, row in news.iterrows():
                # Grab the html content
                html_content = grab_html(row["url"])

                # Convert the publishedDate to UTC
                conversion_result = std_article_time(row["publishedDate"], row["site"])
                if conversion_result[1] == "converted":
                    publishedDate = conversion_result[0]
                else:
                    publishedDate = row["publishedDate"]

                document = {
                    "unique_id": row["url"],
                    # Floor timestamp to the nearest minute
                    "timestamp": publishedDate,
                    "timestamp_conversion_result": conversion_result[1],
                    "ticker": row["ticker"],
                    "article_id": "N/A",
                    "headline": row["title"],
                    "article_source": row["site"],
                    "url": row["url"],
                    "summary": row["text"],
                    "article_created_at": publishedDate,
                    "article_updated_at": publishedDate,
                    "article_tickers": [row["ticker"]],
                    "author": "N/A",
                    "content": "N/A",
                    "images": row["image"],
                    "html": html_content,
                    "source": "fmp",
                    "created_at": datetime.now(
                        timezone.utc
                    ),  # Document creation time in UTC
                }

                # Check if the document exists
                existing_doc = collection.find_one(
                    {
                        "unique_id": document["unique_id"],
                        "timestamp": document["timestamp"],
                        "ticker": document["ticker"],
                    }
                )
                if existing_doc:
                    # Already loaded article for this ticker, do nothing
                    continue
                elif len(document["content"]) == 0:
                    continue
                else:
                    # Insert the document directly
                    collection.insert_one(document)
                    logger.info(
                        f"Inserted document for {ticker} at {document['created_at']}"
                    )

            page += 1

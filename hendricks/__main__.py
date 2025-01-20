"""
Main module for the Hendricks API.
"""

import sys
import os
import logging
import signal
from functools import wraps
import dotenv
from flask import Flask, request, jsonify

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quantum_trade_utilities.core.get_path import get_path  # pylint: disable=C0413
from quantum_trade_utilities.io.logging_config import (
    setup_logging,
)  # pylint: disable=C0413

from hendricks.ingest_quotes.load_quote_data import DataLoader  # pylint: disable=C0413
from hendricks.ingest_fmpEPs.load_fmp_data import FinLoader  # pylint: disable=C0413
from hendricks.ingest_news.load_news_data import NewsLoader  # pylint: disable=C0413
from hendricks.ingest_social.load_social_data import (
    SocialLoader,
)  # pylint: disable=C0413

dotenv.load_dotenv(get_path("env"))

app = Flask(__name__)


def handle_sigterm(*args):
    """Handle SIGTERM signal."""
    print("Received SIGTERM, shutting down gracefully...")
    # Perform any cleanup here
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)

# Set up logging configuration
setup_logging()


def requires_api_key(f):
    """Decorator to require an API key for a route."""

    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            api_key = request.headers.get("x-api-key")
            if not api_key:
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "API key is missing",
                            "error_type": "authentication",
                        }
                    ),
                    401,
                )

            if api_key != os.getenv("HENDRICKS_API_KEY"):
                logging.warning(f"Invalid API key attempt: {api_key[:8]}...")
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Invalid API key",
                            "error_type": "authentication",
                        }
                    ),
                    403,
                )

            return f(*args, **kwargs)

        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Authentication system error",
                        "error_type": "system",
                    }
                ),
                500,
            )

    return decorated


@app.route("/hendricks/load_quotes", methods=["POST"])
@requires_api_key
def load_quotes():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    logging.info(f"Received data: {data}")

    tickers = data.get("tickers")
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    minute_adjustment = data.get("minute_adjustment")
    mongo_db = data.get("mongo_db")

    if minute_adjustment is None:
        minute_adjustment = True

    collection_name = data.get("collection_name")
    if collection_name is None:
        collection_name = "rawPriceColl"

    source = data.get("source")
    if source is None:
        source = "fmp"
    else:
        source = source[0]
        print(f"Source: {source}")

    if not tickers:
        return jsonify({"error": "Ticker symbol is required"}), 400
    if not source:
        return jsonify({"error": "Source is required"}), 400

    failed_tickers = []
    successful_tickers = []

    # Process each ticker individually
    for ticker in tickers:
        try:
            loader = DataLoader(
                tickers=[ticker],  # Process one ticker at a time
                from_date=from_date,
                to_date=to_date,
                collection_name=collection_name,
                source=source,
                minute_adjustment=minute_adjustment,
                mongo_db=mongo_db,
            )
            loader.load_quote_data()
            successful_tickers.append(ticker)
        except Exception as e:
            logging.error(f"Error loading ticker {ticker}: {e}")
            failed_tickers.append({"ticker": ticker, "error": str(e)})
            continue  # Continue with next ticker even if this one fails

    # Return detailed status
    return (
        jsonify(
            {
                "status": "completed",
                "successful_tickers": successful_tickers,
                "failed_tickers": failed_tickers,
                "collection": collection_name,
            }
        ),
        202,
    )


@app.route("/hendricks/load_fin_data", methods=["POST"])
@requires_api_key
def load_fin_data():
    # TODO: Make endpoint names consistent throughout __main__.py
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    logging.info(f"Received data: {data}")

    tickers = data.get("tickers")
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    sources = data.get("sources")
    mongo_db = data.get("mongo_db")

    collection_name = data.get("collection_name")
    fmp_endpoint = data.get("target_endpoint")

    # Daily flag is for processing daily data by day
    daily_fmp_flag = data.get("daily_fmp_flag")

    logging.info(f"Tickers: {tickers}")
    logging.info(f"From date: {from_date}")
    logging.info(f"To date: {to_date}")
    logging.info(f"Collection name: {collection_name}")
    logging.info(f"Target endpoint: {fmp_endpoint}")
    logging.info(f"Daily FMP flag: {daily_fmp_flag}")

    if sources is None:
        sources = ["fmp"]

    if not tickers:
        return jsonify({"error": "Ticker symbol is required"}), 400
    if not fmp_endpoint:
        return jsonify({"error": "FMP endpoint is required"}), 400

    failed_tickers = []
    successful_tickers = []

    # Process each ticker individually
    for ticker in tickers:
        for source in sources:
            logging.info(
                f"Trying to instantiate FinLoader for {fmp_endpoint} for {ticker} from {source}"
            )
            try:
                loader = FinLoader(
                    tickers=[ticker],  # Process one ticker at a time
                    from_date=from_date,
                    to_date=to_date,
                    collection_name=collection_name,
                    source=source,
                    fmp_endpoint=fmp_endpoint,
                    mongo_db=mongo_db,
                )
                # * USING FROM_DATE TO CONTROL DAILY LOADING
                if daily_fmp_flag:
                    logging.info(f"Running load_daily_fin_data for {ticker}")
                    loader.load_daily_fin_data()
                else:
                    logging.info(f"Running load_agg_fin_data for {ticker}")
                    loader.load_agg_fin_data()
                successful_tickers.append(ticker)
            except Exception as e:
                logging.error(f"Error loading ticker {ticker}: {e}")
                failed_tickers.append({"ticker": ticker, "error": str(e)})
                continue  # Continue with next ticker even if this one fails

    # Return detailed status
    return (
        jsonify(
            {
                "status": "completed",
                "successful_tickers": successful_tickers,
                "failed_tickers": failed_tickers,
                "collection": collection_name,
            }
        ),
        202,
    )


@app.route("/hendricks/load_news", methods=["POST"])
@requires_api_key
def load_news():
    """Endpoint to load news articles into the database."""
    data = request.json
    print(f"Received data: {data}")

    tickers = data.get("tickers")
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    collection_name = data.get("collection_name")
    mongo_db = data.get("mongo_db")

    if collection_name is None:
        collection_name = "rawNewsColl"
    sources = data.get("sources")
    gridfs_bucket = data.get("gridfs_bucket")

    if not tickers:
        return jsonify({"error": "Ticker symbol is required"}), 400
    if not sources:
        return jsonify({"error": "Source are required"}), 400

    failed_sources = []
    successful_sources = []

    for source in sources:
        try:
            loader = NewsLoader(
                tickers=tickers,
                from_date=from_date,
                to_date=to_date,
                collection_name=collection_name,
                source=source,
                gridfs_bucket=gridfs_bucket,
                mongo_db=mongo_db,
            )
            loader.load_news_data()
            successful_sources.append(source)
        except Exception as e:
            logging.error(f"Error loading source {source}: {e}")
            failed_sources.append({"source": source, "error": str(e)})
            continue  # Continue with next source even if this one fails

    return (
        jsonify(
            {
                "status": "completed",
                "successful_sources": successful_sources,
                "failed_sources": failed_sources,
                "collection": collection_name,
            }
        ),
        202,
    )


@app.route("/hendricks/load_social", methods=["POST"])
@requires_api_key
def load_social():
    """Endpoint to load social media data into the database."""
    data = request.json
    logging.info(f"Received data: {data}")

    tickers = data.get("tickers")
    collection_name = data.get("collection_name")
    mongo_db = data.get("mongo_db")

    # Social-specific parameters
    sources = data.get("sources")  # Default to reddit
    subreddits = data.get("subreddits")
    reddit_load = data.get("reddit_load")  # Default to recent mode
    comment_depth = data.get("comment_depth", 100)
    keywords = data.get("keywords", {})  # New parameter for keyword mappings
    target_endpoint = data.get("target_endpoint")

    # Set default collection name if none provided
    if collection_name is None:
        collection_name = "rawSocial"

    if not tickers:
        return jsonify({"error": "Ticker symbol is required"}), 400
    if not sources:
        return jsonify({"error": "Social source endpoint is required"}), 400

    # Send immediate response that processing has started
    response = {
        "status": "processing",
        "message": f"Started processing {len(tickers)} tickers for {sources}",
        "collection": collection_name,
        "reddit_load": reddit_load,
    }

    # Start processing in background
    try:
        for source in sources:
            loader = SocialLoader(
                tickers=tickers,
                collection_name=collection_name,
                source=source,
                subreddits=subreddits,
                reddit_load=reddit_load,
                mongo_db=mongo_db,
                comment_depth=comment_depth,
                keywords=keywords,
                target_endpoint=target_endpoint,
            )
            loader.load_data()
            logging.info(f"Successfully processed source: {source}")

    except Exception as e:
        logging.error(f"Error in social data processing: {str(e)}")
        # Processing continues even if there's an error

    return (
        jsonify(response),
        202,
    )  # 202 Accepted indicates the request is being processed


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8711)

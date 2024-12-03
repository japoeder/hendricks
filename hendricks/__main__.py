"""
Main module for the Hendricks API.
"""

import sys
import os
import logging
import json
import signal
from functools import wraps
import dotenv
from flask import Flask, request, jsonify

dotenv.load_dotenv()
# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hendricks.load_ticker_data import DataLoader  # pylint: disable=C0413
from hendricks.stream_ticker_data import DataStreamer  # pylint: disable=C0413
from hendricks._utils.get_path import get_path  # pylint: disable=C0413


def handle_sigterm(*args):
    """Handle SIGTERM signal."""
    print("Received SIGTERM, shutting down gracefully...")
    # Perform any cleanup here
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)

app = Flask(__name__)


app_log_path = get_path("log")
# Configure logging
logging.basicConfig(
    filename=app_log_path,
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s",
)

# Add console handler for logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s"
)
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

logging.debug("This is a test log message.")


def requires_api_key(f):
    """Decorator to require an API key for a route."""

    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get("x-api-key")
        if not api_key or api_key != os.getenv("HENDRICKS_API_KEY"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)

    return decorated


@app.route("/load_tickers", methods=["POST"])
@requires_api_key
def load_tickers():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    print(f"Received data: {data}")
    tickers = data.get("tickers")
    if not tickers:
        return jsonify({"error": "Ticker symbol is required"}), 400

    # Trigger background task to load ticker data
    # If file is provided, load from file, otherwise load from Alpaca API

    file = data.get("file")
    if file is None:
        file = False

    from_date = data.get("from_date")
    if from_date is None:
        from_date = False

    to_date = data.get("to_date")
    if to_date is None:
        to_date = False

    collection_name = data.get("collection_name")
    if collection_name is None:
        collection_name = "rawPriceColl"

    batch_size = data.get("batch_size")
    if batch_size is None:
        batch_size = 7500

    loader = DataLoader(
        tickers=tickers,
        file=file,
        from_date=from_date,
        to_date=to_date,
        collection_name=collection_name,
        batch_size=batch_size,
    )

    loader.load_ticker_data()
    return (
        jsonify(
            {"status": f"{tickers} dataframe loaded into {collection_name} collection."}
        ),
        202,
    )


@app.route("/stream_load", methods=["POST"])
def stream_load():
    """Endpoint to stream a ticker from the Alpaca API."""
    try:
        data = request.json
        logging.debug(f"Received data: {data}")
        tickers = data.get("tickers")
        if not tickers:
            try:
                # Detect OS and read job ctrl from appropriate .env var + /stream_load_ctrl.json
                job_ctrl_path = get_path("job_ctrl")
                with open(job_ctrl_path, "r", encoding="utf-8") as file:
                    job_ctrl = json.load(file)

                # Get stream load from request
                stream_ctrl = data.get("stream_load")

                # Get ticker symbols from stream ctrl
                tickers = job_ctrl.get(stream_ctrl)

            except Exception:
                return jsonify({"error": "Ticker symbols are required "}), 400

        # Get collection name from request, default to streamPriceColl
        collection_name = data.get("collection_name", "streamPriceColl")

        data_loader = DataLoader(tickers=tickers, collection_name=collection_name)
        data_streamer = DataStreamer(tickers=tickers, collection_name=collection_name)
        data_streamer.start_streaming(data_loader)

        return (
            jsonify({"status": f"Streaming started for tickers {tickers}."}),
            202,
        )
    except Exception as e:
        logging.error(f"Error during stream load: {e}")
        return jsonify({"error": "An internal error occurred"}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8001)

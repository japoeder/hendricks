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
from hendricks.qc_historical_quote_alpacaAPI import run_qc  # pylint: disable=C0413
from hendricks._utils.get_path import get_path  # pylint: disable=C0413


def handle_sigterm(*args):
    """Handle SIGTERM signal."""
    print("Received SIGTERM, shutting down gracefully...")
    # Perform any cleanup here
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_sigterm)

app = Flask(__name__)
# Configure logging
logging.basicConfig(
    filename="/Users/jpoeder/dataservices/Documents/pydev/quantum_trade/hendricks/hendricks/app.log",
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


@app.route("/load_ticker", methods=["POST"])
@requires_api_key
def load_ticker():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    print(f"Received data: {data}")
    ticker_symbols = data.get("ticker_symbols")
    if not ticker_symbols:
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

    load_ticker_data = DataLoader(
        ticker_symbols=ticker_symbols,
        file=file,
        from_date=from_date,
        to_date=to_date,
        collection_name=collection_name,
        batch_size=batch_size,
    )

    load_ticker_data.load_historical()
    return (
        jsonify(
            {
                "status": f"{ticker_symbols} dataframe loaded into {collection_name} collection."
            }
        ),
        202,
    )


@app.route("/run_qc", methods=["POST"])
@requires_api_key
def run_quality_control():
    """Endpoint to run quality control on a ticker at a given timestamp."""
    data = request.json
    ticker = data.get("ticker")
    from_date = data.get("from_date")

    # If from_date isn't provided, set to False, and run QC on all timestamps
    if from_date is None:
        from_date = False

    to_date = data.get("to_date")
    if to_date is None:
        to_date = False

    # If ticker isn't provided, set to False, and run QC on all tickers
    if ticker is None:
        ticker = False

    run_qc(ticker=ticker, from_date=from_date, to_date=to_date)

    return (
        jsonify(
            {
                "message": f"QC task for {ticker} from {from_date} to {to_date} has been completed"
            }
        ),
        200,
    )


@app.route("/stream_load", methods=["POST"])
def stream_load():
    """Endpoint to stream a ticker from the Alpaca API."""
    try:
        data = request.json
        logging.debug(f"Received data: {data}")
        ticker_symbols = data.get("ticker_symbols")
        if not ticker_symbols:
            try:
                # Detect OS and read job ctrl from appropriate .env var + /stream_load_ctrl.json
                job_ctrl_path = get_path("job_ctrl")
                with open(job_ctrl_path, "r", encoding="utf-8") as file:
                    job_ctrl = json.load(file)

                # Get stream load from request
                stream_ctrl = data.get("stream_load")

                # Get ticker symbols from stream ctrl
                ticker_symbols = job_ctrl.get(stream_ctrl)

            except Exception:
                return jsonify({"error": "Ticker symbols are required "}), 400

        # Get collection name from request, default to streamPriceColl
        collection_name = data.get("collection_name", "streamPriceColl")

        data_loader = DataLoader(
            ticker_symbols=ticker_symbols, collection_name=collection_name
        )
        data_streamer = DataStreamer(
            ticker_symbols=ticker_symbols, collection_name=collection_name
        )
        data_streamer.start_streaming(data_loader)

        return (
            jsonify({"status": f"Streaming started for tickers {ticker_symbols}."}),
            202,
        )
    except Exception as e:
        logging.error(f"Error during stream load: {e}")
        return jsonify({"error": "An internal error occurred"}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8001)

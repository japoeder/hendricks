from flask import Flask, request, jsonify
from dotenv import load_dotenv
load_dotenv()
import sys
import os
# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hendricks.load_ticker_data import DataLoader
from hendricks.stream_ticker_data import DataStreamer
from hendricks.load_historical_quote_alpacaAPI import load_historical_quote_alpacaAPI
from hendricks.qc_historical_quote_alpacaAPI import run_qc
import logging
import pandas as pd
import argparse
import asyncio


app = Flask(__name__)
# Configure logging
logging.basicConfig(filename='/Users/jpoeder/dataservices/Documents/pydev/quantum_trade/hendricks/hendricks/app.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

# Add console handler for logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

logging.debug("This is a test log message.")

#TODO: update to return meta data in response (can prob. use timestame to control)
@app.route("/load_ticker", methods=["POST"])
def load_ticker():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    ticker_symbols = data.get("ticker_symbols")
    if not ticker_symbol:
        return jsonify({"error": "Ticker symbol is required"}), 400

    # Trigger background task to load ticker data
    # If file is provided, load from file, otherwise load from Alpaca API

    ticker_symbol = data.get("ticker_symbol")
    if ticker_symbol is None:
        ticker_symbol = False

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

    load_ticker_data = DataLoader(ticker_symbols=ticker_symbols,
                                     file=file,
                                     from_date=from_date,
                                     to_date=to_date,
                                     collection_name=collection_name,
                                     batch_size=batch_size)
    
    load_ticker_data.load_data()
    return jsonify({"status": f"{ticker_symbols} dataframe loaded into {collection_name} collection."}), 202

@app.route('/run_qc', methods=['POST'])
def run_quality_control():
    data = request.json
    ticker = data.get('ticker')
    timestamp = data.get('timestamp')

    if not ticker or not timestamp:
        return jsonify({"error": "Ticker and timestamp are required"}), 400

    run_qc(ticker=ticker, timestamp=timestamp)

    return jsonify({"message": f"QC task for {ticker} at {timestamp} has been started"}), 200


@app.route("/stream_load", methods=["POST"])
def stream_load():
    try:
        data = request.json
        logging.debug(f"Received data: {data}")
        ticker_symbols = data.get("ticker_symbols")
        if not ticker_symbols:
            return jsonify({"error": "Ticker symbols are required"}), 400

        #TODO: modify to allow collection name to be passed in but default to streamPriceColl
        # Need to update script that calls flask application to allow for this
        collection_name = data.get("collection_name", "streamPriceColl")

        data_loader = DataLoader(ticker_symbols=ticker_symbols, collection_name=collection_name)
        data_streamer = DataStreamer(ticker_symbols=ticker_symbols, collection_name=collection_name)
        data_streamer.start_streaming(data_loader)

        return jsonify({"status": f"Streaming started for tickers {ticker_symbols}."}), 202
    except Exception as e:
        logging.error(f"Error during stream load: {e}")
        return jsonify({"error": "An internal error occurred"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8001)
    # load_historical_quote_alpacaAPI(
    #     ticker_symbol='AAPL',
    #     collection_name='rawPriceColl',
    #     from_date='2017-03-25T00:00',
    #     to_date='2017-04-3T23:59',
    #     batch_size=7500,
    #     creds_file_path="/Users/jpoeder/pydev/quantum_trade/_cred/creds.json"
    # )
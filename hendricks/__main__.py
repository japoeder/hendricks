from flask import Flask, request, jsonify
from hendricks.load_ticker_data import DataLoader
import logging
import pandas as pd

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route("/load_ticker", methods=["POST"])
def load_ticker():
    """Endpoint to load a new stock ticker into the database."""
    data = request.json
    ticker_symbol = data.get("ticker_symbol")
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
        collection_name = "historicalPrices"

    batch_size = data.get("batch_size")
    if batch_size is None:
        batch_size = 7500

    load_ticker_data = DataLoader(
        ticker_symbol=ticker_symbol,
        file=file,
        from_date=from_date,
        to_date=to_date,
        collection_name=collection_name,
        batch_size=batch_size,
    )

    load_ticker_data.load_data()
    return (
        jsonify(
            {
                "status": f"{ticker_symbol} dataframe loaded into {collection_name} collection."
            }
        ),
        202,
    )


# @app.route("/run_qc", methods=["POST"])
# def run_quality_control():
#     """Endpoint to run QC on the data."""
#     data = request.json
#     ticker_symbol = data.get("ticker_symbol")
#     if not ticker_symbol:
#         return jsonify({"error": "Ticker symbol is required"}), 400

#     # Trigger background task to run QC
#     run_qc.delay(ticker_symbol)
#     return jsonify({"status": "QC started"}), 202

# @app.route("/stream_data", methods=["POST"])
# def stream_data_endpoint():
#     """Endpoint to start streaming data into the database."""
#     data = request.json
#     ticker_symbol = data.get("ticker_symbol")
#     if not ticker_symbol:
#         return jsonify({"error": "Ticker symbol is required"}), 400

#     # Trigger background task to stream data
#     stream_data.delay(ticker_symbol)
#     return jsonify({"status": "Data streaming started"}), 202

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8001)

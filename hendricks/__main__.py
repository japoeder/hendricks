from flask import Flask, request, jsonify
import logging

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
    load_ticker_data.delay(ticker_symbol)
    return jsonify({"status": "Ticker data loading started"}), 202

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
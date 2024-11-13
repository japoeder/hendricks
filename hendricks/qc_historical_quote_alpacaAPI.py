import logging
from datetime import datetime, timezone, timedelta
import pytz
from dotenv import load_dotenv
load_dotenv()
from alpaca_trade_api import REST
from _utils.mongo_conn import mongo_conn
from _utils.mongo_coll_verification import confirm_mongo_collect_exists
from _utils.load_credentials import load_credentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_qc(ticker=None, 
           collection='rawPriceColl', 
           backup_collection='rawPriceColl_bak',
           creds_file_path="/home/jonathan/pydev/quantum_trade/_cred/creds.json",
           timestamp=None):
    """
    Perform quality control checks on the historical quote data.
    """
    # Step 1: Connect to MongoDB 'stocksDB'
    db = mongo_conn()

    # Step 2: Confirm collection exists
    confirm_mongo_collect_exists(collection)
    confirm_mongo_collect_exists(backup_collection)

    # Step 3: Determine the date range
    if timestamp:
        # Convert the timestamp string to a datetime object
        try:
            timestamp = datetime.fromisoformat(timestamp)
        except ValueError:
            logging.error("Invalid timestamp format. Expected ISO format.")
            return

        # If a specific timestamp is provided, use it
        min_date = timestamp.date()
        current_date = timestamp.date()
    else:
        # Otherwise, find the earliest timestamp in the collection
        min_timestamp_doc = db[collection].find_one(sort=[('timestamp', 1)])
        if not min_timestamp_doc:
            logging.info("No data found in the collection.")
            return
        min_date = min_timestamp_doc['timestamp'].date()
        current_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    # Step 4: Iterate through each ticker and timestamp
    tickers = [ticker] if ticker else db[collection].distinct('ticker')
    for ticker in tickers:
        current_time = datetime.combine(min_date, datetime.min.time(), timezone.utc)
        end_time = datetime.combine(current_date, datetime.max.time(), timezone.utc)

        while current_time <= end_time:
            # Define the query for the current minute and ticker
            query = {'timestamp': current_time, 'ticker': ticker}

            # Fetch the document for the current minute and ticker
            document = db[collection].find_one(query)
            if document:
                logging.info(f"Analyzing {ticker} at {current_time}")
                logging.info(f"Current DB values: {document}")

                # Perform QC check
                start_utc = current_time
                end_utc = start_utc + timedelta(minutes=1)

                # Fetch data from Alpaca API
                API_KEY, API_SECRET, BASE_URL = load_credentials(creds_file_path)
                api = REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")
                bars = api.get_bars(ticker, '1Min', start=start_utc.isoformat(), end=end_utc.isoformat()).df

                # Compare and update if necessary
                if not bars.empty:
                    row = bars.iloc[0]
                    if (document['open'] != row['open'] or
                        document['low'] != row['low'] or
                        document['high'] != row['high'] or
                        document['close'] != row['close'] or
                        document['volume'] != row['volume'] or
                        document.get('trade_count', 0) != row.get('trade_count', 0) or
                        document.get('vwap', 0) != row.get('vwap', 0)):

                        # Archive the old document
                        try:
                            document['archived_at'] = datetime.now(timezone.utc)
                            db[backup_collection].insert_one(document)
                            logging.info(f"Successfully archived old data for {ticker} at {current_time}")
                        except Exception as e:
                            logging.error(f"Failed to archive old data for {ticker} at {current_time}: {e}")

                        # Update the document with new data
                        new_document = {
                            "ticker": ticker,
                            "timestamp": current_time,
                            "open": row["open"],
                            "low": row["low"],
                            "high": row["high"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "trade_count": row.get("trade_count", 0),
                            "vwap": row.get("vwap", 0),
                            "created_at": datetime.now(timezone.utc),
                        }
                        try:
                            db[collection].replace_one({'_id': document['_id']}, new_document)
                            logging.info(f"Successfully updated document for {ticker} at {current_time}")
                        except Exception as e:
                            logging.error(f"Failed to update document for {ticker} at {current_time}: {e}")

            # Move to the next minute
            current_time += timedelta(minutes=1)
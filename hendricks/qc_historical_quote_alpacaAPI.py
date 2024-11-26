"""
Module for performing quality control checks on historical quote data.
"""

import logging
from datetime import datetime, timezone, timedelta
from alpaca_trade_api import REST
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import (
    confirm_mongo_collect_exists,
)
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.get_path import get_path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_qc(
    ticker=None,
    collection="rawPriceColl",
    backup_collection="rawPriceColl_bak",
    creds_file_path=None,
    from_date=None,
    to_date=None,
):
    """
    Perform quality control checks on the historical quote data.
    """
    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Step 1: Connect to MongoDB 'stocksDB'
    db = mongo_conn()

    # Step 2: Confirm collection exists
    confirm_mongo_collect_exists(collection)
    confirm_mongo_collect_exists(backup_collection)

    # Step 4: Iterate through each ticker and timestamp
    tickers = [ticker] if ticker else db[collection].distinct("ticker")
    for t in tickers:
        logging.info(f"Running QC on {t}")

        if from_date:
            logging.info(f"Using provided from_date: {from_date}")
            # Convert the timestamp string to a datetime object
            try:
                from_date = datetime.fromisoformat(from_date).replace(
                    tzinfo=timezone.utc
                )
                # If a specific timestamp is provided, use it
                min_datetime = from_date
            except ValueError:
                logging.error("Invalid timestamp format. Expected ISO format.")
                return
            if to_date:
                logging.info(f"Using provided to_date: {to_date}")
                # Convert the timestamp string to a datetime object
                try:
                    max_datetime = datetime.fromisoformat(to_date).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    logging.error("Invalid timestamp format. Expected ISO format.")
                    return
            else:
                # Otherwise, use the current time as the max date minus 1 minute
                max_datetime = datetime.now(timezone.utc) - timedelta(minutes=1)
        else:
            # Otherwise, find the earliest timestamp in the collection
            min_timestamp_doc = db[collection].find_one(sort=[("timestamp", 1)])
            if not min_timestamp_doc:
                logging.info("No data found in the collection.")
                return
            min_datetime = db[collection].find_one(
                sort=[("timestamp", 1), ("ticker", 1)]
            )["timestamp"]
            max_datetime = db[collection].find_one(
                sort=[("timestamp", -1), ("ticker", 1)]
            )["timestamp"]

        current_time = min_datetime
        end_time = max_datetime

        while current_time <= end_time:
            # Define the query for the current minute and ticker
            query = {"timestamp": current_time, "ticker": t}

            # Fetch the document for the current minute and ticker
            document = db[collection].find_one(query)

            # Perform QC check
            start_utc = current_time
            end_utc = start_utc + timedelta(minutes=1)
            logging.info(f"start_utc: {start_utc}")
            logging.info(f"end_utc: {end_utc}")

            # Fetch data from Alpaca API
            API_KEY, API_SECRET, BASE_URL = load_credentials(creds_file_path)
            api = REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")
            bars = api.get_bars(
                t, "1Min", start=start_utc.isoformat(), end=end_utc.isoformat()
            ).df

            if not bars.empty:
                row = bars.iloc[0]

                if document:
                    # Compare and update if necessary
                    logging.info(f"Comparing: {document}")
                    logging.info("With")
                    logging.info(f"{row}")

                    if (
                        document["open"] != row["open"]
                        or document["low"] != row["low"]
                        or document["high"] != row["high"]
                        or document["close"] != row["close"]
                        or document["volume"] != row["volume"]
                        or document.get("trade_count", 0) != row.get("trade_count", 0)
                        or document.get("vwap", 0) != row.get("vwap", 0)
                    ):
                        logging.info(f"Running update on {t} at {current_time}")
                        # Archive the old document
                        try:
                            document["archived_at"] = datetime.now(timezone.utc)
                            db[backup_collection].insert_one(document)
                            logging.info(
                                f"Successfully archived old data for {t} at {current_time}"
                            )
                        except Exception as e:
                            logging.error(
                                f"Failed to archive old data for {t} at {current_time}: {e}"
                            )

                        # Update the document with new data
                        new_document = {
                            "ticker": t,
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
                            db[collection].replace_one(
                                {"_id": document["_id"]}, new_document
                            )
                            logging.info(
                                f"Successfully updated document for {t} at {current_time}"
                            )
                        except Exception as e:
                            logging.error(
                                f"Failed to update document for {t} at {current_time}: {e}"
                            )

                else:
                    # Insert new document if it doesn't exist
                    new_document = {
                        "ticker": t,
                        "timestamp": current_time,
                        "open": float(row["open"]),
                        "low": float(row["low"]),
                        "high": float(row["high"]),
                        "close": float(row["close"]),
                        "volume": float(row["volume"]),
                        "trade_count": float(row.get("trade_count", 0)),
                        "vwap": float(row.get("vwap", 0)),
                        "created_at": datetime.now(timezone.utc),
                    }
                    logging.info(f"{new_document}")
                    try:
                        db[collection].insert_one(new_document)
                        logging.info(
                            f"Successfully inserted new document for {t} at {current_time}"
                        )
                    except Exception as e:
                        logging.error(
                            f"Failed to insert new document for {t} at {current_time}: {e}"
                        )

            # Move to the next minute
            current_time += timedelta(minutes=1)

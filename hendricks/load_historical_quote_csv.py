"""
Load historical quotes from a CSV file into a MongoDB collection.
"""

import csv
from _utils.mongo_conn import mongo_conn


def load_historical_quote_csv(ticker_symbol, raw_data_path, collection_name):
    """
    Load historical quotes from a CSV file into a MongoDB collection.
    """
    # Get the database connection
    db = mongo_conn()
    collection = db[collection_name]

    # Path to your CSV file
    csv_file_path = f"{raw_data_path}"

    # Read CSV and insert into MongoDB
    with open(csv_file_path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Add the ticker column
            row["ticker"] = ticker_symbol
            # Convert the timestamp to a datetime object if necessary
            # row['timestamp'] = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
            collection.update_one(
                {
                    "timestamp": row["timestamp"],
                    "ticker": row["ticker"],
                },  # Use timestamp and ticker as unique identifiers
                {"$set": row},
                upsert=True,
            )

    print("Data imported successfully!")

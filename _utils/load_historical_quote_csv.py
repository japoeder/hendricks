"""
Load historical quotes from a CSV file into a MongoDB collection.
"""

import csv
from _utils.mongo_conn import mango_conn


def load_historical_quotes_csv(ticker_symbol, raw_data_path):
    """
    Load historical quotes from a CSV file into a MongoDB collection.
    """
    # Get the database connection
    db = mango_conn()
    collection = db["historicalPrices"]

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

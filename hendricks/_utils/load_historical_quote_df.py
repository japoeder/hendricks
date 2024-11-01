"""
Load a historical quote dataframe into a MongoDB collection.
"""

from datetime import datetime, timezone
from _utils.mongo_conn import mango_conn
from _utils.mongo_coll_verification import confirm_mongo_collect_exists


def load_historical_quote_df(df, ticker_symbol, collection_name, batch_size=7500):
    """
    Load a historical quote dataframe into a MongoDB collection.
    """
    # Prepare the DataFrame
    df = df.copy()  # Work on a copy to avoid modifying the original DataFrame
    df.columns = df.columns.str.lower()
    df["ticker"] = ticker_symbol

    # Get the database connection
    db = mango_conn()

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name)

    # Get the collection
    collection = db[collection_name]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Prepare documents for batch insert
    documents = []
    for _, row in df.iterrows():
        document = {
            "ticker": row["ticker"],
            "timestamp": row["timestamp"],
            "open": row["open"],
            "low": row["low"],
            "high": row["high"],
            "close": row["close"],
            "volume": row["volume"],
            "trade_count": row.get("trade_count", 0),  # Default to 0 if not present
            "vwap": row.get("vwap", 0),  # Default to 0 if not present
            "created_at": datetime.now(timezone.utc),  # Document creation time in UTC
        }
        documents.append(document)

        # Insert in batches
        if len(documents) >= batch_size:
            collection.insert_many(documents)
            documents = []

    # Insert any remaining documents
    if documents:
        collection.insert_many(documents)

    print("Data imported successfully!")

"""
Load stream quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime, timezone

from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists


def stream_from_alpacaAPI(
    stream_data,
    collection_name,
    creds_file_path,
    mongo_db: str = "stocksDB",
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """
    # Get the database connection
    db = mongo_conn(mongo_db=mongo_db)

    # Ensure the collection exists
    confirm_mongo_collect_exists(collection_name, mongo_db)

    # Get the collection
    collection = db[collection_name]

    # Create a compound index on 'timestamp' and 'ticker'
    collection.create_index([("timestamp", 1), ("ticker", 1)], unique=True)

    # Construct the document to be stored in MongoDB
    document = {
        "ticker": stream_data.get("S"),  # Ticker symbol
        "timestamp": stream_data.get("t"),  # Timestamp of the trade
        "price": stream_data.get("p"),  # Price of the trade
        "size": stream_data.get("s"),  # Size of the trade
        "exchange": stream_data.get("x"),  # Exchange where the trade occurred
        "trade_id": stream_data.get("i"),  # Unique trade identifier
        "conditions": stream_data.get("c"),  # Conditions of the trade
        "created_at": datetime.now(timezone.utc),  # Document creation time in UTC
    }

    # Here you would insert the document into your MongoDB collection
    collection.insert_one(document)

"""
MongoDB connection utility for Gilfoyle.
"""

from dotenv import load_dotenv
from pymongo import MongoClient
from hendricks._utils.load_credentials import load_credentials

load_dotenv()


def mongo_conn():
    """
    Connect to MongoDB.
    """
    mongo_user, mongo_password, mongo_host, mongo_port = load_credentials("mongo_ds")
    conn = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/stocksDB?directConnection=true&serverSelectionTimeoutMS=2000&authSource=admin&appName=mongosh+2.3.2"
    # Connect to MongoDB
    client = MongoClient(conn)
    db = client["stocksDB"]
    return db

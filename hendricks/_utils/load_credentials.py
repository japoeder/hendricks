"""
Load Alpaca API credentials from JSON file.
"""
import json
from dotenv import load_dotenv

load_dotenv()


def load_alpaca_credentials(file_path, data_type):
    """
    Load Alpaca API credentials from JSON file.
    """
    if data_type == "paper_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_paper_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["PAPER_URL"],
        )

    elif data_type == "live_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_live_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["LIVE_URL"],
        )

    elif data_type == "news":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        news_creds = creds["alpaca_news_api"]
        return (
            news_creds["API_KEY"],
            news_creds["API_SECRET"],
        )

    else:
        raise ValueError(f"Invalid data type: {data_type}")

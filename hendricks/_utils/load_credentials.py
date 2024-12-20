"""
Load Alpaca API credentials from JSON file.
"""
import json
from dotenv import load_dotenv

load_dotenv()


def load_credentials(file_path, data_type):
    """
    Load Alpaca API credentials from JSON file.
    """

    if data_type == "alpaca_paper_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_paper_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["PAPER_URL"],
        )

    elif data_type == "alpaca_live_trade":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        alpaca_creds = creds["alpaca_live_api"]
        return (
            alpaca_creds["API_KEY"],
            alpaca_creds["API_SECRET"],
            alpaca_creds["LIVE_URL"],
        )

    elif data_type == "alpaca_news":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        news_creds = creds["alpaca_news_api"]
        return (
            news_creds["API_KEY"],
            news_creds["API_SECRET"],
        )

    if data_type == "fmp_api":
        with open(file_path, "r", encoding="utf-8") as file:
            creds = json.load(file)
        fmp_creds = creds["fmp_api"]
        return (
            fmp_creds["API_KEY"],
            fmp_creds["BASE_URL"],
        )

    else:
        raise ValueError(f"Invalid data type: {data_type}")

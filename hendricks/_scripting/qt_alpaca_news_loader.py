"""
Load historical ticker data into MongoDB.
"""

import argparse
import os
import json
import requests

# Default values
# URL = "https://poederhome.myvnc.com/load_news"
URL = "http://localhost:8001/load_news"
FROM_DATE = "2024-10-03T09:30:00-04:00"
TO_DATE = "2024-10-04T00:59:32-04:00"
BATCH_SIZE = 50000
COLLECTION_NAME = "rawNewsColl"
TICKERS = "AAPL"  # Default ticker
ARTICLES_LIMIT = 50
NEWS_SOURCE = "alpaca"

# Check if API key is set
QT_HENDRICKS_API_KEY = os.getenv("QT_HENDRICKS_API_KEY")
if QT_HENDRICKS_API_KEY is None:
    print("Error: QT_HENDRICKS_API_KEY environment variable is not set.")
    exit(1)


# Help function
def show_help():
    """
    Show the help message.
    """
    print(
        "Usage: python qt_alpaca_news_loader.py -t ticker_symbols [-f file] [-s from_date] [-e to_date] [-c collection_name] [-b batch_size]"
    )
    print()
    print("Options:")
    print("  -t    Comma-separated list of ticker symbols (required)")
    print("  -s    From date (default: {})".format(FROM_DATE))
    print("  -e    To date (default: {})".format(TO_DATE))
    print("  -c    Collection name (default: {})".format(COLLECTION_NAME))
    print("  -b    Batch size (default: {})".format(BATCH_SIZE))
    print("  -a    Articles limit (default: {})".format(ARTICLES_LIMIT))
    print("  -n    Source (default: {})".format(NEWS_SOURCE))
    print("  -h    Show this help message")


# Parse command-line arguments
parser = argparse.ArgumentParser(description="Load historical ticker data.")
parser.add_argument(
    "-t",
    "--tickers",
    type=str,
    required=True,
    help="Comma-separated list of ticker symbols",
)
parser.add_argument(
    "-s",
    "--from_date",
    type=str,
    default=FROM_DATE,
    help="From date (default: {})".format(FROM_DATE),
)
parser.add_argument(
    "-e",
    "--to_date",
    type=str,
    default=TO_DATE,
    help="To date (default: {})".format(TO_DATE),
)
parser.add_argument(
    "-c",
    "--collection_name",
    type=str,
    default=COLLECTION_NAME,
    help="Collection name (default: {})".format(COLLECTION_NAME),
)
parser.add_argument(
    "-b",
    "--batch_size",
    type=int,
    default=BATCH_SIZE,
    help="Batch size (default: {})".format(BATCH_SIZE),
)
parser.add_argument(
    "-a",
    "--articles_limit",
    type=int,
    default=ARTICLES_LIMIT,
    help="Articles limit (default: {})".format(ARTICLES_LIMIT),
)
parser.add_argument(
    "-n",
    "--news_source",
    type=str,
    default=NEWS_SOURCE,
    help="News source (default: {})".format(NEWS_SOURCE),
)
args = parser.parse_args()

# Check if ticker symbols are provided
if not args.tickers:
    print("Error: Ticker symbols are required")
    show_help()
    exit(1)

# Convert comma-separated tickers to JSON array format
tickers_list = args.tickers.split(",")
tickers_json = json.dumps(tickers_list)

# Prepare the data payload
data = {
    "tickers": tickers_list,
    "from_date": args.from_date,
    "to_date": args.to_date,
    "collection_name": args.collection_name,
    "batch_size": args.batch_size,
    "articles_limit": args.articles_limit,
    "source": args.news_source,
}

# Define the headers
headers = {"Content-Type": "application/json", "x-api-key": QT_HENDRICKS_API_KEY}

# Send the POST request to the Flask server
response = requests.post(URL, json=data, headers=headers)

# Print the response from the server
print(response.text)
print(response.status_code)

# Optionally print the JSON response
try:
    print(response.json())
except ValueError:
    print("Response is not in JSON format.")

"""
Run quality control on historical ticker data.
"""

import argparse
import os
import requests

# Default values
URL = "https://poederhome.myvnc.com/run_qc"
FROM_DATE = "2016-01-01T00:00:00Z"
BATCH_SIZE = 50000

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
        "Usage: python qt_run_qc.py -t ticker_symbols [-s from_date] [-e to_date] [-b batch_size]"
    )
    print()
    print("Options:")
    print(
        "  -t    Comma-separated list of ticker symbols (optional, if not provided, QC will run on all tickers)"
    )
    print("  -s    From date (default: {})".format(FROM_DATE))
    print("  -e    To date (optional)")
    print("  -b    Batch size (default: {})".format(BATCH_SIZE))
    print("  -h    Show this help message")


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Run quality control on historical ticker data."
)
parser.add_argument(
    "-t",
    "--tickers",
    type=str,
    default=None,
    help="Comma-separated list of ticker symbols (optional)",
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
    default=None,
    help="To date (optional)",
)
args = parser.parse_args()

# Prepare the data payload
data = {
    "tickers": args.tickers.split(",")
    if args.tickers
    else None,  # Use None if no tickers are provided
    "from_date": args.from_date,
    "to_date": args.to_date,
}

# Define the headers
headers = {"Content-Type": "application/json", "x-api-key": QT_HENDRICKS_API_KEY}

# Send the POST request to the Flask server
try:
    response = requests.post(
        URL, json=data, headers=headers, timeout=6000
    )  # 10 seconds timeout
    response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)

    # Print the response from the server
    print("Response Status Code:", response.status_code)
    print("Response Text:", response.text)

    # Optionally print the JSON response
    try:
        print("JSON Response:", response.json())
    except ValueError:
        print("Response is not in JSON format.")
except requests.exceptions.HTTPError as err:
    print(f"HTTP error occurred: {err}")
except requests.exceptions.Timeout:
    print("The request timed out")
except requests.exceptions.RequestException as err:
    print(f"An error occurred: {err}")

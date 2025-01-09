"""
Load historical quote data from Alpaca API into a MongoDB collection.
"""

from datetime import datetime

# from datetime import timezone
import logging

# import pytz
import hashlib
import pandas as pd
from dotenv import load_dotenv
import requests
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

# from gridfs import GridFS

load_dotenv()
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists
from hendricks._utils.get_path import get_path
from hendricks._utils.request_url_constructor import request_url_constructor
from hendricks._utils.propcase import propcase

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def fsIncomeStmt_from_fmpAPI(
    tickers=None,
    collection_name=None,
    creds_file_path=None,
    from_date=None,
    to_date=None,
    ep=None,
):
    """
    Load historical quote data from Alpaca API into a MongoDB collection.
    """

    ep_timestamp_field = "acceptedDate"
    cred_key = "fmp_api_findata"

    if creds_file_path is None:
        creds_file_path = get_path("creds")

    # Load Alpaca API credentials from JSON file
    API_KEY, BASE_URL = load_credentials(creds_file_path, cred_key)

    # Get the database connection
    db = mongo_conn()

    coll_grp = "fs"
    periods = ["annual", "quarter"]

    for ticker in tickers:
        for period in periods:
            coll_name_pd = f"{collection_name.split('_')[0]}_{coll_grp}{propcase(period)}{collection_name.split('_')[1]}"

            # Ensure the collection exists
            confirm_mongo_collect_exists(coll_name_pd)

            # Get the collection
            collection = db[coll_name_pd]

            # Create indexes for common query patterns
            collection.create_index([("timestamp", 1)])  # For date range queries
            collection.create_index([("ticker", 1)])  # For ticker queries
            collection.create_index([("unique_id", 1)])  # For source filtering

            collection.create_index(
                [("ticker", 1), ("timestamp", -1)]
            )  # For ticker + time sorting
            collection.create_index(
                [("unique_id", 1), ("timestamp", -1)]
            )  # For source + time sorting

            # Uniqueness constraint
            collection.create_index(
                [("unique_id", 1), ("ticker", 1)],
                unique=True,
                background=True,  # Allow other operations while building index
            )

            url = request_url_constructor(
                endpoint=ep,
                base_url=BASE_URL,
                ticker=ticker,
                api_key=API_KEY,
                source="fmp",
                from_date=from_date,
                to_date=to_date,
                period=period,
            )

            print(f"URL: {url}")

            # Get the news data
            response = requests.get(url)
            logger.info(f"FMP API URL: {url}")
            logger.info(f"FMP API Response Status: {response.status_code}")

            if response.status_code == 200:
                res = response.json()
                logger.info(f"FMP API Response Length: {len(res)}")

                if len(res) == 0:
                    logger.info(f"No data found for {ticker}")
                    continue

                # Convert news list of dictionaries to a pandas DataFrame
                res_df = pd.DataFrame(res)
                logger.info(f"DataFrame shape: {res_df.shape}")
                logger.info(f"DataFrame columns: {res_df.columns.tolist()}")

                # Sort results by timestamp in descending order
                res_df.sort_values(by=ep_timestamp_field, ascending=False, inplace=True)

                # Process news items in bulk
                bulk_operations = []
                for _, row in res_df.iterrows():
                    if ep_timestamp_field == "today":
                        # timestamp = datetime.now(timezone.utc)
                        timestamp = datetime.now()
                    elif ep_timestamp_field == "year":
                        # Jan 1st of the year
                        # timestamp = datetime(int(row["year"]), 1, 1, tzinfo=timezone.utc)
                        timestamp = datetime(int(row["year"]), 1, 1)
                    else:
                        # Handle any other timestamp field
                        timestamp = (
                            pd.to_datetime(row[ep_timestamp_field])
                            # .tz_localize("America/New_York")
                            # .tz_convert("UTC")
                        )

                    # Create a hash of the actual estimate values to detect changes
                    feature_values = {
                        "revenue": row["revenue"],
                        "costOfRevenue": row["costOfRevenue"],
                        "grossProfit": row["grossProfit"],
                        "grossProfitRatio": row["grossProfitRatio"],
                        "researchAndDevelopmentExpenses": row[
                            "researchAndDevelopmentExpenses"
                        ],
                        "generalAndAdministrativeExpenses": row[
                            "generalAndAdministrativeExpenses"
                        ],
                        "sellingAndMarketingExpenses": row[
                            "sellingAndMarketingExpenses"
                        ],
                        "sellingGeneralAndAdministrativeExpenses": row[
                            "sellingGeneralAndAdministrativeExpenses"
                        ],
                        "otherExpenses": row["otherExpenses"],
                        "operatingExpenses": row["operatingExpenses"],
                        "costAndExpenses": row["costAndExpenses"],
                        "interestIncome": row["interestIncome"],
                        "interestExpense": row["interestExpense"],
                        "depreciationAndAmortization": row[
                            "depreciationAndAmortization"
                        ],
                        "ebitda": row["ebitda"],
                        "ebitdaratio": row["ebitdaratio"],
                        "operatingIncome": row["operatingIncome"],
                        "operatingIncomeRatio": row["operatingIncomeRatio"],
                        "totalOtherIncomeExpensesNet": row[
                            "totalOtherIncomeExpensesNet"
                        ],
                        "incomeBeforeTax": row["incomeBeforeTax"],
                        "incomeBeforeTaxRatio": row["incomeBeforeTaxRatio"],
                        "incomeTaxExpense": row["incomeTaxExpense"],
                        "netIncome": row["netIncome"],
                        "netIncomeRatio": row["netIncomeRatio"],
                        "eps": row["eps"],
                        "epsdiluted": row["epsdiluted"],
                        "weightedAverageShsOut": row["weightedAverageShsOut"],
                        "weightedAverageShsOutDil": row["weightedAverageShsOutDil"],
                    }
                    feature_hash = hashlib.sha256(
                        str(feature_values).encode()
                    ).hexdigest()

                    # created_at = datetime.now(timezone.utc)
                    created_at = datetime.now()

                    # Create unique_id when there isn't a good option in response
                    f1 = row["date"]
                    f2 = period
                    f3 = row["calendarYear"]
                    f4 = row["acceptedDate"]
                    f5 = created_at

                    # Create hash of f1, f2, f3, f4, f5
                    unique_id = hashlib.sha256(
                        f"{f1}{f2}{f3}{f4}{f5}".encode()
                    ).hexdigest()

                    # Streamlined main document
                    document = {
                        "unique_id": unique_id,
                        "timestamp": timestamp,
                        "ticker": row["symbol"],
                        ##########################################
                        ##########################################
                        "date": row["date"],
                        "reportedCurrency": row["reportedCurrency"],
                        "cik": row["cik"],
                        "fillingDate": row["fillingDate"],
                        "acceptedDate": row["acceptedDate"],
                        "calendarYear": row["calendarYear"],
                        "period": row["period"],
                        **feature_values,
                        "feature_hash": feature_hash,
                        "link": row["link"],
                        "finalLink": row["finalLink"],
                        ##########################################
                        ##########################################
                        "source": "fmp",
                        "created_at": created_at,
                    }

                    bulk_operations.append(
                        UpdateOne(
                            # Check records by date (and other record identifiers) and if feature_hash is different
                            {
                                "calendarYear": row["calendarYear"],
                                "period": row["period"],
                                "feature_hash": {"$ne": feature_hash},
                            },
                            # If identifiers exists exists and feature_hash is different, update record
                            {"$set": document},
                            # If identifiers don't exist, insert new record
                            upsert=True,
                        )
                    )

                # Execute bulk operations if any exist
                if bulk_operations:
                    try:
                        result = collection.bulk_write(bulk_operations, ordered=False)
                        logger.info(
                            f"Processed {len(bulk_operations)} news items for {ticker}"
                        )
                        logger.info(
                            f"Inserted: {result.upserted_count}, Modified: {result.modified_count}"
                        )
                    except BulkWriteError as bwe:
                        logger.warning(
                            f"Some writes failed for {ticker}: {bwe.details}"
                        )

                logger.info("Data imported successfully!")

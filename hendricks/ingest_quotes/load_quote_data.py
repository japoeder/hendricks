"""
Load ticker data into MongoDB.
"""

from datetime import timedelta
import dotenv
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from quantum_trade_utilities.core.get_path import get_path

from hendricks.ingest_quotes.quote_from_alpacaAPI import quote_from_alpacaAPI
from hendricks.ingest_quotes.quote_from_fmpAPI import quote_from_fmpAPI
from hendricks.stream_quotes.stream_from_alpacaAPI import stream_from_alpacaAPI

dotenv.load_dotenv()


class DataLoader:
    """
    Load ticker data into MongoDB.
    """

    def __init__(
        self,
        tickers: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = "rawPriceColl",
        batch_size: int = 7500,
        source: str = None,
        minute_adjustment: bool = True,
        mongo_db: str = "stocksDB",
    ):
        self.tickers = tickers
        self.from_date = pd.to_datetime(from_date)
        self.to_date = pd.to_datetime(to_date) if to_date else pd.Timestamp.now()
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.creds_file_path = get_path("creds")
        self.source = source
        self.minute_adjustment = minute_adjustment
        self.mongo_db = mongo_db
        # Create US business day calendar
        self.us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    def is_trading_day(self, date):
        """Check if a given date is a trading day."""
        # Convert to pandas timestamp if not already
        date = pd.Timestamp(date)

        # Check if it's a weekend
        if date.weekday() in [5, 6]:  # Saturday = 5, Sunday = 6
            return False

        # Check if it's a holiday
        calendar = USFederalHolidayCalendar()
        holidays = calendar.holidays(start=date, end=date)
        if len(holidays) > 0:
            return False

        return True

    # TODO: Incorporate logic from lfd_enum.py and load_fmp_data.py for consistency
    def load_quote_data(self):
        """Load ticker data into MongoDB day by day."""
        current_date = self.from_date

        while current_date <= self.to_date:
            # Skip non-trading days
            if not self.is_trading_day(current_date):
                print(f"Skipping non-trading day: {current_date.strftime('%Y-%m-%d')}")
                current_date += timedelta(days=1)
                continue

            # Format date as string for API calls
            date_str = current_date.strftime("%Y-%m-%d")
            next_date = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")

            print(f"Processing data for {date_str}")

            try:
                if self.source == "alpaca":
                    print(f"Fetching data from Alpaca API for {self.tickers}")
                    quote_from_alpacaAPI(
                        tickers=self.tickers,
                        collection_name=self.collection_name,
                        from_date=date_str,
                        to_date=next_date,
                        creds_file_path=self.creds_file_path,
                        minute_adjustment=self.minute_adjustment,
                        mongo_db=self.mongo_db,
                    )
                elif self.source == "fmp":
                    print(f"Fetching data from FMP API for {self.tickers}")
                    quote_from_fmpAPI(
                        tickers=self.tickers,
                        collection_name=self.collection_name,
                        from_date=date_str,
                        to_date=next_date,
                        creds_file_path=self.creds_file_path,
                        mongo_db=self.mongo_db,
                    )
                else:
                    raise ValueError("Unsupported source")

                print(f"Completed processing for {date_str}")

            except Exception as e:
                print(f"Error processing {date_str}: {str(e)}")
                # Continue to next day even if current day fails

            # Move to next day
            current_date += timedelta(days=1)

        return None

    def load_stream_doc(self, stream_list):
        """Process and store streaming data into MongoDB."""
        stream_from_alpacaAPI(
            stream_data=stream_list,
            collection_name=self.collection_name,
            creds_file_path=self.creds_file_path,
            mongo_db=self.mongo_db,
        )
        print("Data imported successfully!")

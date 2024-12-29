"""
Load ticker data into MongoDB.
"""

# from datetime import timedelta
import dotenv
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from hendricks.ingest_finData.empCount_from_fmpAPI import empCount_from_fmpAPI
from hendricks._utils.get_path import get_path

dotenv.load_dotenv()


class FinLoader:
    """
    Load ticker data into MongoDB.
    """

    def __init__(
        self,
        tickers: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = None,
        batch_size: int = 7500,
        source: str = None,
        minute_adjustment: bool = True,
        endpoint: dict = None,
    ):
        self.tickers = tickers
        self.from_date = pd.to_datetime(from_date)
        self.to_date = pd.to_datetime(to_date) if to_date else pd.Timestamp.now()
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.creds_file_path = get_path("creds")
        self.source = source
        self.minute_adjustment = minute_adjustment
        self.endpoint = endpoint

        # Create US business day calendar
        self.us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    def load_fin_data(self):
        """Load ticker data into MongoDB day by day."""
        # current_date = self.from_date

        print(f"Processing data for {self.tickers}")

        if self.source == "fmp":
            if self.endpoint == "employee_count":
                print(f"Fetching employee count data from FMP API for {self.tickers}")
                empCount_from_fmpAPI(
                    tickers=self.tickers,
                    collection_name=self.collection_name,
                    creds_file_path=self.creds_file_path,
                )
            else:
                raise ValueError("Unsupported endpoint")
        else:
            raise ValueError("Unsupported source")

        print(f"Completed processing for {self.tickers}")

        return None

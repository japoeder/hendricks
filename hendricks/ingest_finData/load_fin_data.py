"""
Load ticker data into MongoDB.
"""

# from datetime import timedelta
import logging
import dotenv
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from hendricks.ingest_finData.empCount_from_fmpAPI import empCount_from_fmpAPI
from hendricks.ingest_finData.execComp_from_fmpAPI import execComp_from_fmpAPI
from hendricks.ingest_finData.grade_from_fmpAPI import grade_from_fmpAPI
from hendricks.ingest_finData.marketCap_from_fmpAPI import marketCap_from_fmpAPI
from hendricks.ingest_finData.analystEst_from_fmpAPI import analystEst_from_fmpAPI
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
        fmp_endpoint: dict = None,
    ):
        self.tickers = tickers
        self.from_date = pd.to_datetime(from_date)
        self.to_date = pd.to_datetime(to_date) if to_date else pd.Timestamp.now()
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.creds_file_path = get_path("creds")
        self.source = source
        self.minute_adjustment = minute_adjustment
        self.fmp_endpoint = fmp_endpoint

        # Create US business day calendar
        self.us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    def load_agg_fin_data(self):
        """Load ticker data into MongoDB day by day."""
        # current_date = self.from_date

        print(f"Processing data for {self.tickers} on endpoint {self.fmp_endpoint}")

        if self.source != "fmp":
            raise ValueError("Unsupported source")

        # Map endpoints to their corresponding functions
        endpoint_handlers = {
            "employee_count": empCount_from_fmpAPI,
            "executive_compensation": execComp_from_fmpAPI,
            "grade": grade_from_fmpAPI,
            "analyst-estimates": analystEst_from_fmpAPI,
        }

        if self.fmp_endpoint not in endpoint_handlers:
            raise ValueError(f"Unsupported endpoint: {self.fmp_endpoint}")

        handler_function = endpoint_handlers[self.fmp_endpoint]
        handler_function(
            tickers=self.tickers,
            collection_name=self.collection_name,
            creds_file_path=self.creds_file_path,
        )

        print(f"Completed processing for {self.tickers}")
        return None

    def load_daily_fin_data(self):
        """Load ticker data into MongoDB day by day."""
        print(f"Processing data for {self.tickers} on endpoint {self.fmp_endpoint}")

        if self.source != "fmp":
            raise ValueError("Unsupported source")

        # Map endpoints to their corresponding functions
        endpoint_handlers = {
            "historical-market-capitalization": marketCap_from_fmpAPI,
            # Add new endpoints here with their corresponding functions
            # "some-other-endpoint": other_endpoint_function,
        }

        if self.fmp_endpoint not in endpoint_handlers:
            raise ValueError(f"Unsupported endpoint: {self.fmp_endpoint}")

        handler_function = endpoint_handlers[self.fmp_endpoint]
        from_date = pd.to_datetime(self.from_date)
        to_date = pd.to_datetime(self.to_date)

        # If from_date and to_date are more than 30 days, loop by month
        if (to_date - from_date).days > 30:
            # Loop by month
            loop_mon_beg = from_date
            loop_mon_end = from_date + pd.DateOffset(months=1)
            while loop_mon_end <= to_date:
                logging.info(f"handler_function: {handler_function}")
                logging.info(f"tickers: {self.tickers}")
                logging.info(f"from_date: {loop_mon_beg.strftime('%Y-%m-%d')}")
                logging.info(f"to_date: {loop_mon_end.strftime('%Y-%m-%d')}")
                logging.info(f"collection_name: {self.collection_name}")

                handler_function(
                    tickers=self.tickers,
                    from_date=loop_mon_beg.strftime("%Y-%m-%d"),
                    to_date=loop_mon_end.strftime("%Y-%m-%d"),
                    collection_name=self.collection_name,
                )

                loop_mon_beg = loop_mon_end
                loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)

                # Handle the final partial month if it exists
                if loop_mon_beg < to_date < loop_mon_end:
                    handler_function(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime("%Y-%m-%d"),
                        to_date=to_date.strftime("%Y-%m-%d"),
                        collection_name=self.collection_name,
                    )
        else:
            # For periods less than 30 days, make a single API call
            handler_function(
                tickers=self.tickers,
                from_date=self.from_date,
                to_date=self.to_date,
                collection_name=self.collection_name,
            )

        print(f"Completed processing for {self.tickers}")
        return None

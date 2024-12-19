"""
Load ticker data into MongoDB.
"""
import logging
import dotenv
import pandas as pd

from hendricks.ingest_quotes.quote_from_alpacaAPI import quote_from_alpacaAPI
from hendricks.ingest_quotes.quote_from_fmpAPI import quote_from_fmpAPI
from hendricks.stream_quotes.stream_from_alpacaAPI import stream_from_alpacaAPI
from hendricks._utils.get_path import get_path

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
    ):
        self.tickers = tickers
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.creds_file_path = get_path("creds")
        self.source = source
        self.minute_adjustment = minute_adjustment

    def load_quote_data(self):
        """Load ticker data into MongoDB."""

        if self.source == "alpaca":
            print(f"Fetching data from Alpaca API for {self.tickers}")
            quote_from_alpacaAPI(
                tickers=self.tickers,
                collection_name=self.collection_name,
                from_date=self.from_date,
                to_date=self.to_date,
                creds_file_path=self.creds_file_path,
                minute_adjustment=self.minute_adjustment,
            )
        elif self.source == "fmp":
            print(f"Fetching data from FMP API for {self.tickers}")
            # Convert string dates to pandas Timestamps
            logging.info(self.from_date)
            logging.info(self.to_date)
            from_date = pd.to_datetime(self.from_date)
            to_date = pd.to_datetime(self.to_date)

            # If from_date and to_date are more than 30 days, loop by month
            if (to_date - from_date).days > 30:
                # Loop by month
                loop_mon_beg = from_date
                loop_mon_end = from_date + pd.DateOffset(months=1)
                while loop_mon_end < to_date:
                    quote_from_fmpAPI(
                        tickers=self.tickers,
                        collection_name=self.collection_name,
                        from_date=loop_mon_beg.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        to_date=loop_mon_end.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        creds_file_path=self.creds_file_path,
                    )
                    loop_mon_beg = loop_mon_end
                    loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)
            else:
                quote_from_fmpAPI(
                    tickers=self.tickers,
                    collection_name=self.collection_name,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    creds_file_path=self.creds_file_path,
                )
        else:
            raise ValueError("Unsupported source")

        return None

    def load_stream_doc(self, stream_list):
        # TODO: need to update logic for processing stream doc
        """Process and store streaming data into MongoDB."""
        stream_from_alpacaAPI(
            stream_data=stream_list,
            collection_name=self.collection_name,
            creds_file_path=self.creds_file_path,
        )
        print("Data imported successfully!")

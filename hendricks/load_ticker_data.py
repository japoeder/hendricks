"""
Load ticker data into MongoDB.
"""
import os
import dotenv
import pandas as pd

from hendricks.quote_from_alpacaAPI import quote_from_alpacaAPI
from hendricks.quote_from_df import quote_from_df
from hendricks.stream_from_alpacaAPI import stream_from_alpacaAPI
from hendricks._utils.get_path import get_path

dotenv.load_dotenv()


class DataLoader:
    """
    Load ticker data into MongoDB.
    """

    def __init__(
        self,
        file: str = None,
        tickers: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = "rawPriceColl",
        batch_size: int = 7500,
    ):
        self.file = file
        self.tickers = tickers
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")
        self.creds_file_path = get_path("creds")

    # Initialize Alpaca API client
    # alpaca_api = REST(API_KEY, API_SECRET, base_url='https://paper-api.alpaca.markets')

    def extension_detection(self, file):
        """Detect the extension of the file."""
        if file.endswith(".pkl"):
            return "pkl"
        else:
            return False

    def load_ticker_data(self):
        """Load ticker data into MongoDB."""
        if not self.file:
            print("No file provided, fetching data from Alpaca API.")
            print(f"Fetching data for {self.tickers}")
            quote_from_alpacaAPI(
                tickers=self.tickers,
                collection_name=self.collection_name,
                from_date=self.from_date,
                to_date=self.to_date,
                creds_file_path=self.creds_file_path,
            )
        else:
            # Process the file
            extension = self.extension_detection(self.file)
            if extension == "pkl":
                df = pd.read_pickle(self.file)
                self.collection_name = str(self.collection_name)
                for ticker in self.tickers:
                    print(f"Loading data from file for {ticker}")
                    quote_from_df(
                        df=df,
                        ticker=ticker,
                        collection_name=self.collection_name,
                        batch_size=self.batch_size,
                    )
            else:
                raise ValueError("Unsupported file type")

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

"""
Load ticker data into MongoDB.
"""
import os
import dotenv
import pandas as pd

from hendricks.load_historical_quote_alpacaAPI import load_historical_quote_alpacaAPI
from hendricks.load_historical_quote_df import load_historical_quote_df
from hendricks.load_stream_quote_alpacaAPI import load_stream_quote_alpacaAPI
from hendricks._utils.get_path import get_path

dotenv.load_dotenv()


class DataLoader:
    """
    Load ticker data into MongoDB.
    """

    def __init__(
        self,
        file: str = None,
        ticker_symbols: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = "rawPriceColl",
        batch_size: int = 7500,
    ):
        self.file = file
        self.ticker_symbols = ticker_symbols
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

    def load_historical(self):
        """Load ticker data into MongoDB."""
        if not self.file:
            print("No file provided, fetching data from Alpaca API.")
            # Fetch data from Alpaca API for each ticker symbol
            for ticker_symbol in self.ticker_symbols:
                print(f"Fetching data for {ticker_symbol}")
                load_historical_quote_alpacaAPI(
                    ticker_symbol=ticker_symbol,
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
                for ticker_symbol in self.ticker_symbols:
                    print(f"Loading data from file for {ticker_symbol}")
                    load_historical_quote_df(
                        df=df,
                        ticker_symbol=ticker_symbol,
                        collection_name=self.collection_name,
                        batch_size=self.batch_size,
                    )
            else:
                raise ValueError("Unsupported file type")

        return None

    def load_stream_doc(self, stream_list):
        # TODO: need to update logic for processing stream doc
        """Process and store streaming data into MongoDB."""
        load_stream_quote_alpacaAPI(
            stream_data=stream_list,
            collection_name=self.collection_name,
            creds_file_path=self.creds_file_path,
        )
        print("Data imported successfully!")

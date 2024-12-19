"""
Load news data into MongoDB.
"""
import dotenv
from hendricks.ingest_news.news_from_alpacaAPI import news_from_alpacaAPI

dotenv.load_dotenv()


class NewsLoader:
    """
    Load ticker data into MongoDB.
    """

    def __init__(
        self,
        file: str = None,
        tickers: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = None,
        batch_size: int = None,
        articles_limit: int = None,
        source: str = None,
    ):
        self.file = file
        self.tickers = tickers
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = batch_size
        self.articles_limit = articles_limit
        self.source = source

    def load_news_data(self):
        """Load news data into MongoDB."""
        if self.source == "alpaca":
            print(f"Fetching data from Alpaca API for {self.tickers}")
            if self.articles_limit is None:
                self.articles_limit = 1
            if self.batch_size is None:
                self.batch_size = 75000
            news_from_alpacaAPI(
                tickers=self.tickers,
                from_date=self.from_date,
                to_date=self.to_date,
                articles_limit=self.articles_limit,
                collection_name=self.collection_name,
            )
        else:
            raise ValueError("Please provide a valid newssource")

        return None

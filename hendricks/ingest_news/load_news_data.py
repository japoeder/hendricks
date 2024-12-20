"""
Load news data into MongoDB.
"""
import dotenv
import pandas as pd
from hendricks.ingest_news.news_from_alpacaAPI import news_from_alpacaAPI
from hendricks.ingest_news.news_from_fmpAPI import news_from_fmpAPI

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
        articles_limit: int = None,
        source: str = None,
    ):
        self.file = file
        self.tickers = tickers
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.articles_limit = articles_limit
        self.source = source

    def load_news_data(self):
        """Load news data into MongoDB."""
        if self.source == "alpaca":
            print(f"Fetching data from Alpaca API for {self.tickers}")
            from_date = pd.to_datetime(self.from_date)
            to_date = pd.to_datetime(self.to_date)

            # If from_date and to_date are more than 30 days, loop by month
            if (to_date - from_date).days > 30:
                # Loop by month
                loop_mon_beg = from_date
                loop_mon_end = from_date + pd.DateOffset(months=1)
                while loop_mon_end < to_date:
                    if self.articles_limit is None:
                        self.articles_limit = 1000
                    news_from_alpacaAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        to_date=loop_mon_end.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        articles_limit=self.articles_limit,
                        collection_name=self.collection_name,
                    )
                    loop_mon_beg = loop_mon_end
                    loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)
            else:
                if self.articles_limit is None:
                    self.articles_limit = 1000
                news_from_alpacaAPI(
                    tickers=self.tickers,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    articles_limit=self.articles_limit,
                    collection_name=self.collection_name,
                )
        elif self.source == "fmp":
            print(f"Fetching data from FMP API for {self.tickers}")
            from_date = pd.to_datetime(self.from_date)
            to_date = pd.to_datetime(self.to_date)

            # If from_date and to_date are more than 30 days, loop by month
            if (to_date - from_date).days > 30:
                # Loop by month
                loop_mon_beg = from_date
                loop_mon_end = from_date + pd.DateOffset(months=1)
                while loop_mon_end < to_date:
                    if self.articles_limit is None:
                        self.articles_limit = 1000
                    news_from_fmpAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        to_date=loop_mon_end.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        articles_limit=self.articles_limit,
                        collection_name=self.collection_name,
                    )
                    loop_mon_beg = loop_mon_end
                    loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)
            else:
                if self.articles_limit is None:
                    self.articles_limit = 1000
                news_from_fmpAPI(
                    tickers=self.tickers,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    articles_limit=self.articles_limit,
                    collection_name=self.collection_name,
                )
        else:
            raise ValueError("Please provide a valid newssource")

        return None

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
        tickers: list = None,
        from_date: str = None,
        to_date: str = None,
        collection_name: str = None,
        source: str = None,
        gridfs_bucket: str = None,
    ):
        self.tickers = tickers
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.source = source
        self.gridfs_bucket = gridfs_bucket

    def load_news_data(self):
        """Load news data into MongoDB."""
        if self.source == "alpaca":
            print(f"Fetching data from Alpaca API for {self.tickers}")
            from_date = pd.to_datetime(self.from_date)
            to_date = pd.to_datetime(self.to_date)

            articles_limit = 50

            # If from_date and to_date are more than 30 days, loop by month
            z = 5
            if (to_date - from_date).days > 30:
                print(f"Running if {z}")
                # Loop by month
                loop_mon_beg = from_date
                loop_mon_end = from_date + pd.DateOffset(months=1)
                while loop_mon_end < to_date:
                    news_from_alpacaAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        to_date=loop_mon_end.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        articles_limit=articles_limit,
                        collection_name=self.collection_name,
                        gridfs_bucket=self.gridfs_bucket,
                    )
                    loop_mon_beg = loop_mon_end
                    loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)
            else:
                print(f"Running else {z}")
                news_from_alpacaAPI(
                    tickers=self.tickers,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    articles_limit=articles_limit,
                    collection_name=self.collection_name,
                    gridfs_bucket=self.gridfs_bucket,
                )
        elif self.source == "fmp":
            print(f"Fetching data from FMP API for {self.tickers}")
            from_date = pd.to_datetime(self.from_date)
            to_date = pd.to_datetime(self.to_date)

            articles_limit = 1000

            # If from_date and to_date are more than 30 days, loop by month
            if (to_date - from_date).days > 30:
                # Loop by month
                loop_mon_beg = from_date
                loop_mon_end = from_date + pd.DateOffset(months=1)
                while loop_mon_end < to_date:
                    news_from_fmpAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        to_date=loop_mon_end.strftime(
                            "%Y-%m-%d"
                        ),  # Convert back to string
                        articles_limit=articles_limit,
                        collection_name=self.collection_name,
                        gridfs_bucket=self.gridfs_bucket,
                    )
                    loop_mon_beg = loop_mon_end
                    loop_mon_end = loop_mon_beg + pd.DateOffset(months=1)
            else:
                articles_limit = 1000
                news_from_fmpAPI(
                    tickers=self.tickers,
                    from_date=self.from_date,
                    to_date=self.to_date,
                    articles_limit=articles_limit,
                    collection_name=self.collection_name,
                    gridfs_bucket=self.gridfs_bucket,
                )
        else:
            raise ValueError("Please provide a valid newssource")

        return None

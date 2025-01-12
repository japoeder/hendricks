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
            from_date = pd.to_datetime(self.from_date).replace(tzinfo=None)
            to_date = pd.to_datetime(self.to_date).replace(tzinfo=None)

            articles_limit = 50

            # Check if dates span different months/years
            if (to_date.year != from_date.year) or (to_date.month != from_date.month):
                print("Dates span multiple months, processing by calendar month")
                # Loop by calendar months
                loop_mon_beg = from_date
                while loop_mon_beg <= to_date:
                    # Get the last day of current month
                    if loop_mon_beg.month == 12:
                        loop_mon_end = pd.Timestamp(
                            year=loop_mon_beg.year + 1, month=1, day=1
                        ) - pd.Timedelta(days=1)
                    else:
                        loop_mon_end = pd.Timestamp(
                            year=loop_mon_beg.year, month=loop_mon_beg.month + 1, day=1
                        ) - pd.Timedelta(days=1)

                    # If we're past the to_date, use to_date as the end
                    loop_mon_end = min(loop_mon_end, to_date)

                    news_from_alpacaAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime("%Y-%m-%d"),
                        to_date=loop_mon_end.strftime("%Y-%m-%d"),
                        articles_limit=articles_limit,
                        collection_name=self.collection_name,
                        gridfs_bucket=self.gridfs_bucket,
                    )

                    # Move to first day of next month
                    if loop_mon_end.month == 12:
                        loop_mon_beg = pd.Timestamp(
                            year=loop_mon_end.year + 1, month=1, day=1
                        )
                    else:
                        loop_mon_beg = pd.Timestamp(
                            year=loop_mon_end.year, month=loop_mon_end.month + 1, day=1
                        )
            else:
                print("Dates within same month, processing as single period")
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
            # Convert dates to timezone-naive timestamps
            from_date = pd.to_datetime(self.from_date).replace(tzinfo=None)
            to_date = pd.to_datetime(self.to_date).replace(tzinfo=None)

            articles_limit = 1000

            # Check if dates span different months/years
            if (to_date.year != from_date.year) or (to_date.month != from_date.month):
                print("Dates span multiple months, processing by calendar month")
                # Loop by calendar months
                loop_mon_beg = from_date
                while loop_mon_beg <= to_date:
                    # Get the last day of current month
                    if loop_mon_beg.month == 12:
                        loop_mon_end = pd.Timestamp(
                            year=loop_mon_beg.year + 1, month=1, day=1
                        ) - pd.Timedelta(days=1)
                    else:
                        loop_mon_end = pd.Timestamp(
                            year=loop_mon_beg.year, month=loop_mon_beg.month + 1, day=1
                        ) - pd.Timedelta(days=1)

                    # If we're past the to_date, use to_date as the end
                    loop_mon_end = min(loop_mon_end, to_date)

                    news_from_fmpAPI(
                        tickers=self.tickers,
                        from_date=loop_mon_beg.strftime("%Y-%m-%d"),
                        to_date=loop_mon_end.strftime("%Y-%m-%d"),
                        articles_limit=articles_limit,
                        collection_name=self.collection_name,
                        gridfs_bucket=self.gridfs_bucket,
                    )

                    # Move to first day of next month
                    if loop_mon_end.month == 12:
                        loop_mon_beg = pd.Timestamp(
                            year=loop_mon_end.year + 1, month=1, day=1
                        )
                    else:
                        loop_mon_beg = pd.Timestamp(
                            year=loop_mon_end.year, month=loop_mon_end.month + 1, day=1
                        )
            else:
                print("Dates within same month, processing as single period")
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

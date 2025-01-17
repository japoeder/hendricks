"""
Load social media data into MongoDB.
"""

import logging
from hendricks.ingest_social.lsd_enum import SocialEndpoint


class SocialLoader:
    """
    Load social media data into MongoDB.
    Handles multiple social platforms with different data structures.
    """

    def __init__(
        self,
        tickers: list = None,
        collection_name: str = None,
        source: str = None,
        subreddits: list = None,
        reddit_load: str = "recent",  # 'recent' or 'live'
        mongo_db: str = "stocksDB",
        comment_depth: int = 100,
    ):
        self.tickers = tickers
        self.collection_name = collection_name
        self.source = source
        self.subreddits = subreddits or ["wallstreetbets", "stocks", "investing"]
        self.reddit_load = reddit_load
        self.mongo_db = mongo_db
        self.comment_depth = comment_depth

    def load_data(self):
        """Load social media data into MongoDB."""
        logging.info(
            f"Processing {self.reddit_load} data for {self.tickers} on {self.source}"
        )

        endpoint = SocialEndpoint.get_by_endpoint(self.source)
        if not endpoint:
            raise ValueError(f"Unsupported social endpoint: {self.source}")

        handler_function = endpoint.function

        # Direct call to handler function
        handler_function(
            tickers=self.tickers,
            collection_name=self.collection_name,
            subreddits=self.subreddits,
            mongo_db=self.mongo_db,
            reddit_load=self.reddit_load,
            verbose=False,
            comment_depth=self.comment_depth,
        )

        logging.info(f"Completed processing for {self.tickers}")
        return None

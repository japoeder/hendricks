"""
Reddit-specific handlers for social media data ingestion.
"""

import logging

from datetime import datetime
import hashlib
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import praw
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

load_dotenv()
from hendricks._utils.mongo_conn import mongo_conn
from hendricks._utils.load_credentials import load_credentials
from hendricks._utils.get_path import get_path
from hendricks._utils.mongo_coll_verification import confirm_mongo_collect_exists

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to suppress DEBUG messages
logger = logging.getLogger("pymongo")
logger.setLevel(logging.WARNING)  # Suppress pymongo debug messages


def socialPosts_from_reddit(
    tickers=None,
    collection_name=None,
    subreddits=None,
    mongo_db="socialDB",
    reddit_load="year",  # 'year' or 'minute'
    verbose=False,
    comment_depth=100,
):
    """Load Reddit data using PRAW."""

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize PRAW with credentials
    CLIENT_ID, CLIENT_SECRET, USER_AGENT, USERNAME, PASSWORD = load_credentials(
        get_path("creds"), "reddit_api"
    )
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        username=USERNAME,
        password=PASSWORD,
    )

    # Get MongoDB connection and setup collections
    db = mongo_conn(mongo_db=mongo_db)
    posts_collection = f"{collection_name}Posts"
    comments_collection = f"{collection_name}Comments"

    confirm_mongo_collect_exists(posts_collection, mongo_db)
    confirm_mongo_collect_exists(comments_collection, mongo_db)

    posts_collection = db[posts_collection]
    comments_collection = db[comments_collection]

    # Create indexes for common query patterns
    posts_collection.create_index([("timestamp", 1)])  # For date range queries
    posts_collection.create_index([("ticker", 1)])  # For ticker queries
    posts_collection.create_index([("unique_id", 1)])  # For source filtering

    posts_collection.create_index(
        [("ticker", 1), ("timestamp", -1)]
    )  # For ticker + time sorting
    posts_collection.create_index(
        [("unique_id", 1), ("timestamp", -1)]
    )  # For source + time sorting

    # Uniqueness constraint
    posts_collection.create_index(
        [("unique_id", 1), ("ticker", 1)],
        unique=True,
        background=True,  # Allow other operations while building index
    )

    # Create indexes for common query patterns
    comments_collection.create_index([("timestamp", 1)])  # For date range queries
    comments_collection.create_index([("ticker", 1)])  # For ticker queries
    comments_collection.create_index([("unique_id", 1)])  # For source filtering

    comments_collection.create_index(
        [("ticker", 1), ("timestamp", -1)]
    )  # For ticker + time sorting
    comments_collection.create_index(
        [("unique_id", 1), ("timestamp", -1)]
    )  # For source + time sorting

    # Uniqueness constraint
    comments_collection.create_index(
        [("unique_id", 1), ("ticker", 1)],
        unique=True,
        background=True,  # Allow other operations while building index
    )

    # Create index on

    time_filter = reddit_load  # Changed from 'hour' to 'day'
    if verbose:
        logger.info(f"Running in {reddit_load} mode with comment depth {comment_depth}")

    for sub in subreddits:
        if verbose:
            logger.info(f"Processing subreddit: {sub}")

        for ticker in tickers:
            if verbose:
                logger.info(f"Searching for ticker: {ticker}")

            try:
                subreddit = reddit.subreddit(sub)

                # Search both with and without the $ symbol
                search_queries = [
                    f'title:"{ticker}" OR selftext:"{ticker}"',
                    f'title:"${ticker}" OR selftext:"${ticker}"',
                ]

                for query in search_queries:
                    submissions = subreddit.search(
                        query, time_filter=time_filter, sort="new", limit=None
                    )

                    bulk_operations = []
                    for submission in submissions:
                        # Create unique_id and feature hash
                        unique_id_fields = f"{submission.created_utc}{submission.title}{submission.author}{submission.subreddit}"
                        unique_id = hashlib.sha256(
                            unique_id_fields.encode()
                        ).hexdigest()

                        feature_values = {
                            "title": submission.title,
                            "selftext": submission.selftext,
                            "score": submission.score,
                            "num_comments": submission.num_comments,
                        }
                        feature_hash = hashlib.sha256(
                            str(feature_values).encode()
                        ).hexdigest()

                        document = {
                            "unique_id": unique_id,
                            "ticker": ticker,
                            "submission_id": submission.id,
                            "timestamp": datetime.fromtimestamp(
                                submission.created_utc, tz=ZoneInfo("UTC")
                            ),
                            **feature_values,
                            "feature_hash": feature_hash,
                            "author": str(submission.author),
                            "subreddit": str(submission.subreddit),
                            "url": submission.url,
                            "created_at": datetime.now(ZoneInfo("UTC")),
                        }

                        bulk_operations.append(
                            UpdateOne(
                                {
                                    "unique_id": document["unique_id"],
                                    "ticker": document["ticker"],
                                    # Only update if hash is different or document doesn't exist
                                    "$or": [
                                        {"feature_hash": {"$ne": feature_hash}},
                                        {"feature_hash": {"$exists": False}},
                                    ],
                                },
                                {"$set": document},
                                upsert=True,
                            )
                        )

                        if len(bulk_operations) >= 500:
                            execute_bulk_operations(
                                posts_collection, bulk_operations, verbose
                            )
                            bulk_operations = []

                        # Process comments with depth based on load type
                        load_reddit_comments(
                            post=submission,
                            post_UID=unique_id,
                            collection=comments_collection,
                            ticker=ticker,
                            comment_depth=comment_depth,  # Pass the comment depth
                            mongo_db=mongo_db,
                            verbose=verbose,
                        )

                    if bulk_operations:
                        execute_bulk_operations(
                            posts_collection, bulk_operations, verbose
                        )

            except Exception as e:
                logger.error(f"Error processing {ticker} in {sub}: {str(e)}")
                continue

    return


def load_reddit_comments(
    post,
    post_UID,
    collection,
    ticker,
    comment_depth=None,
    mongo_db="socialDB",
    verbose=False,
):
    """
    Load Reddit comments for a given post.
    """
    # Initialize PRAW with all credentials
    CLIENT_ID, CLIENT_SECRET, USER_AGENT, USERNAME, PASSWORD = load_credentials(
        get_path("creds"), "reddit_api"
    )
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        username=USERNAME,
        password=PASSWORD,
    )

    bulk_ops = []

    submission_id = post.id

    if verbose:
        print(f"Processing submission ID: {submission_id}")

    # Fetch fresh submission and comments
    submission = reddit.submission(id=submission_id)
    submission.comments.replace_more(limit=comment_depth)

    for comment in submission.comments.list():
        comment_text = comment.body.upper()
        if not (ticker.upper() in comment_text or f"${ticker.upper()}" in comment_text):
            continue

        # Generate unique_id for comment
        unique_id_fields = f"{comment.id}{comment.author}{comment.created_utc}"
        unique_id = hashlib.sha256(unique_id_fields.encode()).hexdigest()

        feature_values = {
            "score": comment.score,
        }
        feature_hash = hashlib.sha256(str(feature_values).encode()).hexdigest()

        # Create comment document with feature hash
        comment_doc = {
            "unique_id": unique_id,
            "post_id": submission_id,
            "post_UID": post_UID,
            "comment_id": comment.id,
            "author": str(comment.author),
            "body": comment.body,
            "timestamp": datetime.fromtimestamp(comment.created_utc),
            "ticker": ticker,
            "subreddit": str(submission.subreddit),
            "created_at": datetime.now(ZoneInfo("UTC")),
            **feature_values,
            "feature_hash": feature_hash,
        }

        bulk_ops.append(
            UpdateOne(
                {
                    "unique_id": comment_doc["unique_id"],
                    "ticker": comment_doc["ticker"],
                    # Only update if hash is different or document doesn't exist
                    "$or": [
                        {"feature_hash": {"$ne": feature_hash}},
                        {"feature_hash": {"$exists": False}},
                    ],
                },
                {"$set": comment_doc},
                upsert=True,
            )
        )

    # Execute bulk operations
    if bulk_ops:
        try:
            result = collection.bulk_write(bulk_ops, ordered=False)
            if verbose:
                print(
                    f"Bulk write results - Inserted: {result.upserted_count}, Modified: {result.modified_count}"
                )
            return len(bulk_ops)
        except BulkWriteError as bwe:
            # Filter out duplicate key errors and only log non-duplicate errors
            non_duplicate_errors = [
                error for error in bwe.details["writeErrors"] if error["code"] != 11000
            ]

            # Only log if there are non-duplicate errors and verbose is True
            if non_duplicate_errors and verbose:
                logger.error(f"Non-duplicate errors occurred: {non_duplicate_errors}")

            # Return number of successful operations
            return bwe.details.get("nUpserted", 0)

    return 0


def execute_bulk_operations(collection, bulk_operations, verbose=False):
    """Execute bulk operations with error handling."""
    try:
        result = collection.bulk_write(bulk_operations, ordered=False)
        if verbose:
            logger.info(
                f"Bulk write results - "
                f"Inserted: {result.upserted_count}, "
                f"Modified: {result.modified_count}"
            )
    except BulkWriteError as bwe:
        # Filter out duplicate key errors (code 11000)
        non_duplicate_errors = [
            error for error in bwe.details["writeErrors"] if error["code"] != 11000
        ]

        # Only log if there are non-duplicate errors
        if non_duplicate_errors and verbose:
            logger.warning(f"Some writes failed: {non_duplicate_errors}")
    return []  # Return empty list to reset bulk_operations

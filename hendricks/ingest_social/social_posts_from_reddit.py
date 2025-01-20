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
    reddit_load="year",
    verbose=True,
    comment_depth=100,
    keywords=None,  # New parameter for additional keywords
    target_endpoint=None,
):
    """Load Reddit data using PRAW."""
    verbose = True
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
        ratelimit_seconds=1,  # Wait 1 second between requests
        timeout=30,  # 30 second timeout for requests
        check_for_async=False,  # Disable async check warning
    )

    # Configure rate limits based on Pro account
    reddit.config.api_request_delay = 1.0  # 1 second between requests
    reddit.config.timeout = 30  # 30 second timeout
    reddit.config.retries = 3  # Retry failed requests up to 3 times

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

    bulk_operations = []  # Initialize bulk_operations list at the top level
    submission_count = 0
    comment_count = 0

    for ticker in tickers:
        if verbose:
            logger.info(f"Processing ticker: {ticker}")

        # Check if a subreddit exists for this ticker
        try:
            ticker_subreddit = reddit.subreddit(ticker)
            # Test if subreddit exists by accessing a property
            _ = ticker_subreddit.display_name
            if verbose:
                logger.info(f"Found dedicated subreddit for {ticker}")

            # Add ticker subreddit to the list for processing
            if subreddits is None:
                subreddits = [ticker]
            else:
                subreddits.append(ticker)

        except Exception as e:
            if verbose:
                logger.info(f"No dedicated subreddit found for {ticker}: {str(e)}")

        # Process all subreddits (including ticker subreddit if found)
        for sub in subreddits:
            if verbose:
                logger.info(f"Processing subreddit: {sub}")

            try:
                subreddit = reddit.subreddit(sub)

                # If this is the ticker's dedicated subreddit, get all posts
                if sub.upper() == ticker.upper():
                    if verbose:
                        logger.info(f"Getting all posts from {ticker}'s subreddit")

                    submissions = subreddit.new(limit=None)
                    # Process all submissions from ticker subreddit
                    bulk_operations = []  # Move this outside the submission loop
                    for submission in submissions:
                        submission_count += 1
                        if verbose and submission_count % 100 == 0:
                            logger.info(
                                f"Processed {submission_count} submissions for {ticker}"
                            )

                        # Create unique_id and process submission
                        unique_id_fields = f"{submission.title}{submission.author}{submission.subreddit}"
                        unique_id = hashlib.sha256(
                            unique_id_fields.encode()
                        ).hexdigest()

                        document = {
                            "unique_id": unique_id,
                            "ticker": ticker,
                            "submission_id": submission.id,
                            "timestamp": datetime.fromtimestamp(
                                submission.created_utc, tz=ZoneInfo("UTC")
                            ),
                            "title": submission.title,
                            "selftext": submission.selftext,
                            "score": submission.score,
                            "num_comments": submission.num_comments,
                            "author": str(submission.author),
                            "subreddit": str(submission.subreddit),
                            "url": submission.url,
                            "created_at": datetime.now(ZoneInfo("UTC")),
                        }

                        bulk_operations.append(
                            UpdateOne(
                                {"unique_id": unique_id},
                                {"$set": document},
                                upsert=True,
                            )
                        )

                        if len(bulk_operations) >= 5:
                            execute_bulk_operations(
                                posts_collection, bulk_operations, verbose
                            )
                            bulk_operations = []

                        # Process comments
                        comment_result = load_reddit_comments(
                            post=submission,
                            post_UID=unique_id,
                            collection=comments_collection,
                            ticker=ticker,
                            comment_depth=comment_depth,
                            mongo_db=mongo_db,
                            verbose=verbose,
                        )
                        comment_count += comment_result

                        if verbose and comment_count % 1000 == 0:
                            logger.info(
                                f"Processed {comment_count} comments for {ticker}"
                            )
                # For other subreddits, use search as before
                else:
                    search_terms = get_ticker_keywords(
                        ticker, keywords.get(ticker) if keywords else None
                    )
                    if verbose:
                        logger.info(f"Using search terms: {search_terms}")

                    for term in search_terms:
                        search_queries = [
                            f'"{term}"',  # Search all fields
                            f'title:"{term}"',  # Specific title search
                            f'selftext:"{term}"',  # Specific selftext search
                            f'flair:"{term}"',  # Search flairs
                        ]

                        for query in search_queries:
                            try:
                                if verbose:
                                    logger.info(f"Executing search query: {query}")

                                submissions = subreddit.search(
                                    query,
                                    time_filter=time_filter,
                                    sort="new",
                                    limit=None,
                                    syntax="lucene",
                                )

                                # Track submissions being processed
                                for submission in submissions:
                                    submission_count += 1
                                    if verbose and submission_count % 100 == 0:
                                        logger.info(
                                            f"Processed {submission_count} submissions for {ticker}"
                                        )

                                    # Create unique_id and feature hash
                                    unique_id_fields = f"{submission.title}{submission.author}{submission.subreddit}"
                                    unique_id = hashlib.sha256(
                                        unique_id_fields.encode()
                                    ).hexdigest()

                                    feature_values = {
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
                                        "title": submission.title,
                                        "selftext": submission.selftext,
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
                                                    {
                                                        "feature_hash": {
                                                            "$ne": feature_hash
                                                        }
                                                    },
                                                    {
                                                        "feature_hash": {
                                                            "$exists": False
                                                        }
                                                    },
                                                ],
                                            },
                                            {"$set": document},
                                            upsert=True,
                                        )
                                    )

                                    if len(bulk_operations) >= 5:
                                        execute_bulk_operations(
                                            posts_collection, bulk_operations, verbose
                                        )
                                        bulk_operations = []

                                    # Track comments
                                    comment_result = load_reddit_comments(
                                        post=submission,
                                        post_UID=unique_id,
                                        collection=comments_collection,
                                        ticker=ticker,
                                        comment_depth=comment_depth,
                                        mongo_db=mongo_db,
                                        verbose=verbose,
                                    )
                                    comment_count += comment_result

                                    if verbose and comment_count % 1000 == 0:
                                        logger.info(
                                            f"Processed {comment_count} comments for {ticker}"
                                        )

                                # Execute any remaining bulk operations
                                if bulk_operations:
                                    execute_bulk_operations(
                                        posts_collection, bulk_operations, verbose
                                    )
                                    bulk_operations = []

                            except Exception as e:
                                logger.error(
                                    f"Error with query '{query}' in {sub}: {str(e)}"
                                )
                                continue

            except Exception as e:
                logger.error(f"Error processing {ticker} in {sub}: {str(e)}")
                continue

    if verbose:
        logger.info(
            f"Final counts - Submissions: {submission_count}, Comments: {comment_count}"
        )
    return


def load_reddit_comments(
    post,
    post_UID,
    collection,
    ticker,
    comment_depth=25,
    mongo_db="socialDB",
    verbose=False,
):
    """Load Reddit comments for a given post."""
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
        ratelimit_seconds=1,  # Wait 1 second between requests
        timeout=30,  # 30 second timeout for requests
        check_for_async=False,  # Disable async check warning
    )

    # Configure rate limits
    reddit.config.api_request_delay = 1.0
    reddit.config.timeout = 30
    reddit.config.retries = 3

    processed_count = 0
    bulk_ops = []

    if verbose:
        logger.info(f"Processing comments for submission {post.id}")

    # Fetch fresh submission and comments
    submission = reddit.submission(id=post.id)
    submission.comments.replace_more(limit=comment_depth)

    all_comments = submission.comments.list()
    if verbose:
        logger.info(f"Found {len(all_comments)} total comments")

    for comment in all_comments:
        processed_count += 1
        # Generate unique_id for comment
        unique_id_fields = f"{comment.id}{comment.author}"
        unique_id = hashlib.sha256(unique_id_fields.encode()).hexdigest()

        feature_values = {
            "score": comment.score,
        }
        feature_hash = hashlib.sha256(str(feature_values).encode()).hexdigest()

        # Create comment document with feature hash and parent info
        comment_doc = {
            "unique_id": unique_id,
            "post_id": post.id,
            "post_UID": post_UID,
            "comment_id": comment.id,
            "parent_id": comment.parent_id,  # Will be 't3_postid' for top-level comments or 't1_commentid' for replies
            "depth": comment.depth,  # Add comment depth in thread
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

    return processed_count


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


"""Keyword mappings for social media searches."""


def get_ticker_keywords(ticker: str, keywords: list = None) -> list:
    """
    Get search keywords for a ticker.
    Args:
        ticker: The stock ticker symbol
        keywords: Optional list of additional keywords to include
    Returns:
        List of search terms including ticker and provided keywords
    """
    search_terms = [
        ticker,  # Base ticker
        f"${ticker}",  # Ticker with $ prefix
    ]

    # Add any provided keywords
    if keywords:
        search_terms.extend(keywords)

    return search_terms

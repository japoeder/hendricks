"""
Enum for social media endpoints and their corresponding handler functions.
"""

from enum import Enum
from hendricks.ingest_social.social_posts_from_reddit import socialPosts_from_reddit


class SocialEndpoint(Enum):
    """
    Enum for social media endpoints.
    Each endpoint maps to specific handler functions and configurations.
    """

    REDDIT_POSTS = {
        "endpoint": "reddit",
        "function": socialPosts_from_reddit,
        "supports_streaming": True,
        "description": "Reddit posts and comments",
    }

    # Add other social platforms here
    # TWITTER_POSTS = {...}
    # STOCKTWITS_POSTS = {...}

    @classmethod
    def get_by_endpoint(cls, source: str):
        """Get endpoint configuration by name."""
        try:
            return next(member for member in cls if member.value["endpoint"] == source)
        except StopIteration:
            return None

    @property
    def function(self):
        """Get the handler function for this endpoint."""
        return self.value["function"]

    @property
    def stream_function(self):
        """Get the streaming function for this endpoint."""
        return self.value["stream_function"]

    @property
    def supports_streaming(self):
        """Check if endpoint supports streaming."""
        return self.value["supports_streaming"]

    @property
    def rate_limit(self):
        """Get rate limit for this endpoint."""
        return self.value["rate_limit"]

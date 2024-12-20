"""
This module contains a function to standardize the article time to UTC.
"""

from datetime import datetime
import logging
import pytz

# TODO: Investigate created at article times vs published at article times
# We want to make sure that time conversion has correct assumptions about
# original timezone and date format.


def std_article_time(date_str, source=None):
    """
    Convert source datetime to UTC and floor to the nearest minute.

    Output string is in the format: "YYYY-MM-DD HH:MM:SS"

    Common formats:
    - Standard US: "April 15, 2024 6:43 PM EDT"
    - ISO: "2024-04-15T18:43:00Z"
    - UK: "15 April 2024 18:43 BST"
    - Wire Services: "Apr 15, 2024 18:43:00 ET"
    """
    if not date_str or not source:
        raise ValueError("date_str and source are required")

    # Clean source name
    source = source.lower().replace("https://", "").replace("www.", "")

    # Initialize timezone info
    et_tz = pytz.timezone("America/New_York")
    uk_tz = pytz.timezone("Europe/London")

    try:
        # Group 1: US Eastern Time sources
        if source in [
            "benzinga.com",
            "benzinga",
            "marketwatch.com",
            "wsj.com",
            "barrons.com",
            "foxbusiness.com",
            "cnbc.com",
            "investors.com",
            "fool.com",
            "investorplace.com",
            "marketbeat.com",
            "seekingalpha.com",
            "zacks.com",
            "247wallst.com",
        ]:
            try:
                local_time = datetime.strptime(date_str, "%B %d, %Y %I:%M %p EDT")
            except ValueError:
                try:
                    local_time = datetime.strptime(date_str, "%Y-%m-%d %I:%M %p")
                except ValueError:
                    try:
                        # Add support for ISO-like format without timezone
                        local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        local_time = datetime.strptime(date_str, "%B %d, %Y %I:%M %p")
            local_time = et_tz.localize(local_time)

        # Group 2: UK/Europe sources
        elif source in ["proactiveinvestors.co.uk", "theguardian.com", "sky.com"]:
            try:
                local_time = datetime.strptime(date_str, "%d %B %Y %H:%M BST")
            except ValueError:
                try:
                    local_time = datetime.strptime(date_str, "%d %B %Y %H:%M GMT")
                except ValueError:
                    try:
                        # Add support for ISO-like format without timezone
                        local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            local_time = uk_tz.localize(local_time)

        # Group 3: Wire services (typically use standardized formats)
        elif source in [
            "businesswire.com",
            "globenewswire.com",
            "prnewswire.com",
            "accesswire.com",
        ]:
            try:
                # Already in UTC/ISO format
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                try:
                    local_time = datetime.strptime(date_str, "%b %d, %Y %H:%M:%S ET")
                except ValueError:
                    try:
                        # Add support for simple timestamp format
                        local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
                local_time = et_tz.localize(local_time)

        # Group 4: Sources that typically use ISO format
        elif source in ["reuters.com", "techcrunch.com", "businessinsider.com"]:
            try:
                # Already in UTC
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                # Fallback to ET
                local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                local_time = et_tz.localize(local_time)

        # Group 5: NYTimes specific
        elif source == "nytimes.com":
            local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            local_time = et_tz.localize(local_time)

        else:
            # Default to ET for unknown sources
            logging.warning(f"Unknown source {source}, defaulting to ET timezone")
            try:
                local_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                local_time = datetime.strptime(date_str, "%B %d, %Y %I:%M %p")
            local_time = et_tz.localize(local_time)

        # Convert to UTC and floor to the nearest minute
        utc_time = local_time.astimezone(pytz.UTC)
        utc_time = utc_time.replace(second=0)
        return [utc_time.strftime("%Y-%m-%d %H:%M:%S"), "converted"]

    except Exception:
        # If the source is not found, return the original date string
        ds = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        ds = ds.replace(second=0)
        return [ds.strftime("%Y-%m-%d %H:%M:%S"), "not_converted"]

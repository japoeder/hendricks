"""
Construct the URL for the FMP API.
"""

from datetime import datetime
import logging
import pytz


def convert_to_utc(date_str, source=None):
    """
    Convert source datetime to UTC.

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
                    local_time = datetime.strptime(date_str, "%B %d, %Y %I:%M %p")
            local_time = et_tz.localize(local_time)

        # Group 2: UK/Europe sources
        elif source in ["proactiveinvestors.co.uk", "theguardian.com", "sky.com"]:
            try:
                local_time = datetime.strptime(date_str, "%d %B %Y %H:%M BST")
            except ValueError:
                local_time = datetime.strptime(date_str, "%d %B %Y %H:%M GMT")
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
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            except ValueError:
                local_time = datetime.strptime(date_str, "%b %d, %Y %H:%M:%S ET")
                local_time = et_tz.localize(local_time)

        # Group 4: Sources that typically use ISO format
        elif source in ["reuters.com", "techcrunch.com", "businessinsider.com"]:
            try:
                # Already in UTC
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
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

        # Convert to UTC
        utc_time = local_time.astimezone(pytz.UTC)
        return utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        logging.error(f"Error converting time for {source}: {str(e)}")
        logging.error(f"Date string was: {date_str}")
        raise ValueError(f"Unable to parse date for {source}: {date_str}")


def request_url_constructor(
    base_url: str = None,
    endpoint: str = None,
    from_date: str = None,
    to_date: str = None,
    api_key: str = None,
    extended: bool = False,
    ticker: str = None,
    interval: str = None,
    source: str = None,
    page: int = None,
    period: str = None,
):
    """
    Construct the URL for the FMP API.
    """
    compiled_url = ""
    if source == "fmp":
        if base_url is None:
            raise ValueError("base_url is required")
        else:
            compiled_url += base_url

        if endpoint is None:
            raise ValueError("endpoint is required")

        elif endpoint == "historical-chart":
            if interval is None:
                raise ValueError("interval is required")
            else:
                compiled_url += f"/{endpoint}/{interval}"

            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{ticker}?"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"apikey={api_key}"

            # Add optional parameters
            if from_date is not None:
                compiled_url += f"&from={from_date}"
                if to_date is not None:
                    compiled_url += f"&to={to_date}"
            if extended is not None:
                compiled_url += f"&extended={extended}"

        elif endpoint == "stock_news":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}?tickers={ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"&apikey={api_key}"

            if page is not None:
                compiled_url += f"&page={page}"

            # Add optional parameters
            if from_date is not None:
                compiled_url += f"&from={from_date}"
                if to_date is not None:
                    compiled_url += f"&to={to_date}"

        elif endpoint == "employee_count":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}?symbol={ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"&apikey={api_key}"

        elif endpoint == "executive_compensation":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}?symbol={ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"&apikey={api_key}"

        elif endpoint == "grade":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}/{ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"?apikey={api_key}"

        elif endpoint == "historical-market-capitalization":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}/{ticker}?"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"&apikey={api_key}"

            # Add optional parameters
            if from_date is not None:
                compiled_url += f"&from={from_date}"
                if to_date is not None:
                    compiled_url += f"&to={to_date}"

        elif endpoint == "analyst-estimates":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}/{ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"?apikey={api_key}"

        elif endpoint == "analyst-stock-recommendations":
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}/{ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"?apikey={api_key}"

        elif endpoint in [
            "income-statement",
            "balance-sheet-statement",
            "cash-flow-statement",
            "key-metrics",
            "ratios",
            "cash-flow-statement-growth",
            "income-statement-growth",
            "balance-sheet-statement-growth",
            "financial-growth",
            "enterprise-values",
        ]:
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}/{ticker}"

            if period is not None:
                compiled_url += f"?period={period}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"&apikey={api_key}"

        elif endpoint in ["score", "owner-earnings"]:
            if ticker is None:
                raise ValueError("ticker is required")
            else:
                compiled_url += f"/{endpoint}?symbol={ticker}"

            if api_key is None:
                raise ValueError("api_key is required")
            else:
                compiled_url += f"?apikey={api_key}"

    return compiled_url

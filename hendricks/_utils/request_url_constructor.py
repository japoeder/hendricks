"""
Construct the URL for the FMP API.
"""


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

    return compiled_url

"""
This module contains a function to grab the HTML content of a given URL.
"""

import logging
import requests
from urllib3.exceptions import InsecureRequestWarning


def grab_html(url):
    """
    Quick attempt to grab HTML content from a URL.
    Will timeout quickly if blocked or slow to respond.
    """
    # Suppress only the specific InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=0.5,  # Half second timeout
            verify=False,  # Skip SSL verification
            allow_redirects=True,
        )
        response.raise_for_status()
        return response.text

    except Exception:
        logging.error(f"Failed quick fetch for URL: {url}")
        return None

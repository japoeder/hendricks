"""
This module contains a function to grab the HTML content of a given URL.
"""

import logging
import random
import requests
from urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import SSLError, RequestException


def grab_html(url):
    """
    Grab HTML content from a URL, handling SSL certificate issues gracefully.
    """
    # List of common user agents to rotate through
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    ]

    # Enhanced headers especially for Reuters
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.google.com/",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }

    # Suppress InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    session = requests.Session()

    # Special handling for Reuters
    if "reuters.com" in url:
        try:
            # Extract article path and clean it
            article_path = url.split("reuters.com")[1].strip("/")

            # Use Reuters ARC API
            arc_url = f"https://www.reuters.com/arc/outboundfeeds/v3/all/{article_path}"

            headers.update(
                {
                    "Authority": "www.reuters.com",
                    "Origin": "https://www.reuters.com",
                    "Referer": url,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )

            response = session.get(
                arc_url, headers=headers, timeout=15, allow_redirects=True
            )
            response.raise_for_status()

            data = response.json()
            if data and "content_elements" in data:
                return data["content_elements"][0]["content"]

            # Fallback to normal GET request if API fails
            response = session.get(
                url, headers=headers, timeout=15, allow_redirects=True
            )
            response.raise_for_status()
            return response.text

        except Exception as e:
            logging.error(f"Failed to fetch Reuters article: {url}")
            logging.error(f"Error: {str(e)}")
            # Continue with normal request handling

    try:
        # First try with normal verification
        response = session.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()
        return response.text

    except SSLError:
        logging.warning(
            f"SSL verification failed for {url}, retrying without verification"
        )
        try:
            response = session.get(
                url, headers=headers, verify=False, timeout=15, allow_redirects=True
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Failed to fetch URL even without SSL verification: {url}")
            logging.error(f"Error: {str(e)}")
            return None

    except RequestException as e:
        logging.error(f"Request failed for URL: {url}")
        logging.error(f"Error: {str(e)}")
        return None

    except Exception as e:
        logging.error(f"Unexpected error fetching URL: {url}")
        logging.error(f"Error: {str(e)}")
        return None

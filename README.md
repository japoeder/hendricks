![Hendricks Banner](https://raw.githubusercontent.com/japoeder/hendricks/main/hendricks/_img/hendricks_banner.jpg)

# Hendricks 🚀

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)

[![MongoDB](https://img.shields.io/badge/MongoDB-4.4%2B-green.svg)](https://www.mongodb.com/)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A robust financial data ingestion service for real-time and historical stock pricing, company news, social data, and more.

## 📋 Table of Contents

- [Hendricks 🚀](#hendricks-)
    - [📋 Table of Contents](#-table-of-contents)
    - [🔍 Overview](#-overview)
    - [✨ Features](#-features)
    - [🛠 Installation](#-installation)
    - [📊 Data Sources](#-data-sources)
        - [Market Quotes](#market-quotes)
        - [News Sources](#news-sources)
    - [📖 Usage](#-usage)
        - [Quote Loader](#quote-loader)
            - [Parameters](#parameters)
        - [Stream Loader](#stream-loader)
        - [News Loader](#news-loader)
    - [📁 Project Structure](#-project-structure)
    - [🔧 Development](#-development)
        - [Service Management](#service-management)
        - [Code Quality](#code-quality)

## 🔍 Overview

Hendricks is a core data loading service designed for efficient financial data ingestion. It provides three primary functionalities:

1. **Batch Loading**: Performs complete data reloads over specified time windows
2. **Quality Control**: Implements automated checks for missing data points and recovery mechanisms
3. **Real-time Streaming**: Enables live market data ingestion into raw price collections

## ✨ Features

- Multi-source data integration (FMP, Alpaca, etc.)
- Real-time and historical data processing
- Automated quality control and data validation
- Flexible API for custom data queries
- Robust error handling and logging
- Command-line interface for easy operation

## 🛠 Installation

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure your API keys (see Configuration section)

## 📊 Data Sources

### Market Quotes

- **Alpaca**: Real-time and historical market data (supplementary to FMP)
- **Financial Modeling Prep (FMP)**: Primary source for market data

### News Sources

- **Financial Modeling Prep (FMP)**: Financial news and analysis
- **Mediastack**: General market news
- **TheNewsAPI**: Additional news coverage

## 📖 Usage

### Quote Loader

```bash
# Load historical quotes
qt_quote_load -t "AAPL,GOOG" -s "2024-11-01T00:00:00Z" -e "2024-11-15T23:59:00Z" -o "fmp"
```

#### Parameters

| Parameter | Description       | Default              | Required |
| --------- | ----------------- | -------------------- | -------- |
| `-t`      | Ticker symbol(s)  | -                    | Yes      |
| `-s`      | Start date        | 2024-10-03T09:30:00Z | No       |
| `-e`      | End date          | Current time         | No       |
| `-c`      | Collection name   | rawPriceColl         | No       |
| `-m`      | Minute adjustment | True                 | No       |
| `-o`      | Data source       | "fmp"                | No       |

### Stream Loader

```bash
# Start real-time data stream
qt_stream_load -t "TSLA" -s "2024-10-03T09:30:00Z"
```

### News Loader

```bash
# Load news articles
qt_news_load -t "TSLA" -s "2024-11-01T00:00:00Z" -e "2024-11-15T23:59:00Z" -a 10 -o "alpaca"
```

> Note: Alpaca news API has a limit of 50 articles per request

## 📁 Project Structure

```
hendricks/
├── README.md         # Project documentation
├── __init__.py       # Package initialization
├── hendricks/        # Core module
│   ├── _utils/       # Utility functions
│   ├── _scripting/   # CLI scripts
│   ├── ingest_news/  # News ingestion logic
│   ├── ingest_quotes/# Quote ingestion logic
│   ├── stream_quotes/# Real-time streaming
│   └── __main__.py   # Entry point
├── .pre-commit-config.yaml
├── .pylintrc
├── pyproject.toml
└── req.txt           # Dependencies
```

## 🔧 Development

### Service Management

```bash
# Restart the service
qt_restart_hl
```

### Code Quality

This project uses:

- Black for code formatting
- Pylint for code analysis
- Pre-commit hooks for consistency
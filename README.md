# Hendricks

### Introduction

This is the core data loading service for the following:

1. Simple load that drops existing records in raw collection, and reloads data over the specified window
2. QC checks missing minutes over a specified window, and attempts to reload
3. Stream load reads live data into the raw price collection

### Service details

1. Restart the service if necessary with:

   - qt_restart_hl

## Data Sources

#### Quotes

- Alpaca (supplement to FMP, needs minute adjustment)
- FMP

#### News

- FMP
- Mediastack
- TheNewsAPI

## Quote Loader

1. Sample quote load request

   - qt_quote_load -t "AAPL,GOOG" -s "2024-11-01T00:00:00Z" -e "2024-11-15T23:59:00Z" -o "fmp"
       - This is a zsh alias that executes a qt_hist_loader in _scripting (though run from scripting in root)
2. Required

   - Tickers - these can be single or a quoted list as above
   - From date

### Usage Details:

```
simple load: qt_hendricks_load via the terminal

optional "parameters "arguments:
  -t    Ticker symbol (required)
  -s    From date (default: 2024-10-03T09:30:00Z)
  -e    To date (default: 2024-10-04T00:59:32Z)
  -c    Collection name (default: rawPriceColl)
  -m    Minute adjustment (default: True)
	-o		Source (default: "fmp")
  -h    Show this help message
```

## Stream Loader

1. Sample stream load request:

   - Dev on pause for the moment.

```
simple load: qt_stream_load via the terminal
  
optional "parameters "arguments:
  -t    Ticker symbol (required)
  -s    From date (default: 2024-10-03T09:30:00Z)
```

## News Loader

1. Sample alpaca news load request:

   - qt_news_load -t "TSLA" -s "2024-11-01T00:00:00Z" -e "2024-11-15T23:59:00Z" -a 10 -n "alpaca"
       - alpaca: max articles is 50
       - for fmp can put what you like

## Python Packaging

This code is packaged as a python module, with the structure outlined in the section below.

### Project Layout

- [hendricks](collectability_model): root.
    - [README.md](README.md):
        - The guide you're reading.
    - [`__init__.py`
        - For package / module structure.
    - [`.`](lab1/init.py)gitignore
        - Version control doc.
    - .pre-commit-config.yaml
        - Pre-commit hooks for formatting and linting.
    - .pylintrc
        - Linter parameter file.
    - pyproject.toml
        - Black exception logic.
    - req.txt
        - Required libraries for model.
    - [hendricks](.): This is the *module* in our *package*.
        - _scripting
            - Directory that holds scripting files to execute POST requests via CLI
        - _utils
            - Directory that holds various utilities the repo relies on
        - `__init__.py`
            - Expose what functions, variables, classes, etc when scripts import this module.
        - [`__main__.py`]
            - This file is the entrypoint to your program when ran as a program.
        - `quote_from_alpacaAPI.py`
            - Logic for loading historical data into MongoDB from the Alpaca API.
        - `quote_from_csv.py`
            - Logic for loading historical data into MongoDB from csv files.
        - `quote_from_df.py`
            - Logic for loading historical data into MongoDB from a pickled dataframe.
        - `load_ticker_data.py`
            - DataLoader class that calls and drives the individual methods above.
        - `stream_ticker_data.py`
            - DataStreamer class that initiates the websocket stream and calls DataLoader for processing and ingestion.
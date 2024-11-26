# Hendricks

### Introduction

This is the core data loading service for the following:

1. Simple load that drops existing records in raw collection, and reloads data over the specified window
2. QC checks missing minutes over a specified window, and attempts to reload
3. Stream load reads live data into the raw price collection

### Service details

1. Restart the service if necessary with:
   * qt_restart_hl

#### Hist Loader

1. Sample historical load request

   * qt_hist_load -t "AAPL,GOOG" -s "2024-11-01T00:00:00Z" -e "2024-11-15T23:59:00Z"

     * This is a zsh alias that executes a qt_hist_loader in _scripting (though run from scripting in root)
2. Tickers are required for a historical load

   * These can be single or a quoted list as above

### Usage Details:

```
simple load: qt_hendricks_load via the terminaloptional "parameters "arguments:
  -t    Ticker symbol (required)
  -s    From date (default: 2024-10-03T09:30:00Z)
  -e    To date (default: 2024-10-04T00:59:32Z)
  -c    Collection name (default: rawPriceColl)
  -b    Batch size (default: 50000)
  -h    Show this help message
```

#### Quality control function

1. Sample qc request:

   * qt_run_qc -t "GOOG, AAPL" -s "2024-01-02T11:00:00Z -e "2024-11-20T11:10:00Z"
   * Note these are UTC times that translate to 5 to 5:10am
2. No requirements for date or ticker.

   * If no ticker or list of tickers is provided, then all tickers in raw data will be checked
   * If no start period is provided all periods in the db are evaluated by ticker.
   * If only the start period is provided, it will run from start period to current period minus 1 minute.
3. If tickers / date provided in the run_qc() request but the data hasn't been loaded via load_ticker(), run_qc() will load missing data.

   * Intuitively, loading historical data is a one and done exercise for a ticker's historical data.  New data should be captured via the stream_load method.
   * This ensures missing periods can be captured after the initial load is performed.

```
simple qc: qt_run_qc via the terminal
  
optional "parameters "arguments:
  -t    Ticker symbol
  -s    From date (default: 2024-10-03T09:30:00Z)
```

## Stream Loader

1. Sample stream load request:
   * qt_hendricks_stream
2. 

```
simple load: qt_stream_load via the terminal
  
optional "parameters "arguments:
  -t    Ticker symbol (required)
  -s    From date (default: 2024-10-03T09:30:00Z)
```

## Python Packaging

This code is packaged as a python module, with the structure outlined in the section below.

### Project Layout

* [hendricks](collectability_model): root.
  * [README.md](README.md):
    The guide you're reading.
  * [`__init__.py`](lab1/__init__.py)
    For package / module structure.
  * [`.`](lab1/__init__.py)gitignore
    Version control doc
  * .pre-commit-config.yaml
    Pre-commit hooks for formatting and linting
  * .pylintrc
    Linter parameter file.
  * [`lint.py`](lab1/__init__.py)
    Linter driver.
  * [`p`](lab1/__init__.py)yproject.toml
    Black exception logic.
  * [`r`](lab1/__init__.py)eq.txt
    Required libraries for model.
  * [hendricks](.): This is the *module* in our *package*.

    * _scripting
      Directory that holds scripting files to execute POST requests via CLI
    * _utils
      Directory that holds various utilities the repo relies on
    * [`__init__.py`](lab1/__init__.py)
      Expose what functions, variables, classes, etc when scripts import this module.
    * [`__main__.py`](lab1/__main__.py)
      This file is the entrypoint to your program when ran as a program.
    * `load_historical_quote_alpacaAPI.py`
      Logic for loading historical data into MongoDB from the Alpaca API.
    * `load_historical_quote_csv.py`
      Logic for loading historical data into MongoDB from csv files.
    * `load_historical_quote_df.py`
      Logic for loading historical data into MongoDB from a pickled dataframe.
    * `load_ticker_data.py`
      DataLoader class that calls and drives the individual methods above.
    * `qc_historical_quote_alpacaAPI.py`
      Logic for running QC of loaded data against the Alpaca API.
    * `stream_ticker_data.py`
      DataStreamer class that initiates the websocket stream and calls DataLoader for processing and ingestion.

"""
Load ticker data into MongoDB.
"""
import os
import dotenv
dotenv.load_dotenv()
from flask import Flask, request, jsonify
import pandas as pd
import pickle
from alpaca_trade_api import REST
import websockets
import json
import asyncio
from hendricks.load_historical_quote_alpacaAPI import load_historical_quote_alpacaAPI
from hendricks.load_historical_quote_df import load_historical_quote_df
from hendricks.load_historical_quote_csv import load_historical_quote_csv
import logging

class DataLoader:
    """
    Load ticker data into MongoDB.
    """
    def __init__(self,
                 file: str = None,
                 ticker_symbol: str = None,
                 from_date: str = None,
                 to_date: str = None,
                 collection_name: str = "rawPriceColl",
                 batch_size: int = 7500
                 ):
        self.file = file
        self.ticker_symbol = ticker_symbol
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")

    # Initialize Alpaca API client
    #alpaca_api = REST(API_KEY, API_SECRET, base_url='https://paper-api.alpaca.markets')

    def extension_detection(self, file):
        """Detect the extension of the file."""
        if file.endswith('.pkl'):
            return 'pkl'
        else:
            return False

    def load_data(self):
        """Load ticker data into MongoDB."""
        if not self.file:
            print("No file provided, fetching data from Alpaca API.")
            # Fetch data from Alpaca API
            load_historical_quote_alpacaAPI(ticker_symbol=self.ticker_symbol,
                                            collection_name=self.collection_name,
                                            from_date=self.from_date,
                                            to_date=self.to_date,
                                            upsert=True)
        else:
            # Process the file
            # TODO: Add data checking of input file against dates in database
            extension = self.extension_detection(self.file)
            if extension == 'pkl':
                df = pd.read_pickle(self.file)
                self.collection_name = str(self.collection_name)
                load_historical_quote_df(df=df,
                                         ticker_symbol=self.ticker_symbol,
                                         collection_name=self.collection_name,
                                         batch_size=self.batch_size)
            else:
                raise ValueError("Unsupported file type")

        return None

    async def stream_data(self):
        uri = "wss://stream.data.alpaca.markets/v2/iex"  # Use the correct endpoint for your data feed
        async with websockets.connect(uri) as websocket:
            print(f"self.API_KEY: {self.API_KEY}")
            print(f"self.API_SECRET: {self.API_SECRET}")
            # Authenticate with Alpaca
            await websocket.send(json.dumps({
                "action": "auth",
                "key": self.API_KEY,
                "secret": self.API_SECRET
            }))
            response = await websocket.recv()
            print("Authentication response:", response)  # Log authentication response

            # Subscribe to the ticker
            await websocket.send(json.dumps({
                "action": "subscribe",
                "trades": [self.ticker_symbol]
            }))
            response = await websocket.recv()
            print("Subscription response:", response)  # Log subscription response

            # Stream data
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print("Received data:", data)  # Log received data

    def start_streaming(self):
        # Use asyncio.run() in a Jupyter-friendly way
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stream_data())

"""
Load ticker data into MongoDB.
"""

from flask import Flask, request, jsonify
import pandas as pd
import pickle
from alpaca_trade_api import REST
import websockets
import json
import asyncio
from _utils.load_historical_quote_alpacaAPI import load_historical_quote_alpacaAPI
from _utils.load_historical_quote_df import load_historical_quote_df
from _utils.load_historical_quote_csv import load_historical_quote_csv


class DataLoader:
    """
    Load ticker data into MongoDB.
    """
    def __init__(self,
                 file: str = None,
                 ticker_symbol: str = None,
                 from_date: str = None,
                 to_date: str = None,
                 collection_name: str = "historicalPrices",
                 batch_size: int = 7500
                 ):
        self.file = file
        self.ticker_symbol = ticker_symbol
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = int(batch_size)

    # Initialize Alpaca API client
    alpaca_api = REST('APCA-API-KEY-ID', 'APCA-API-SECRET-KEY', base_url='https://paper-api.alpaca.markets')

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
                                            to_date=self.to_date)
        else:
            # Process the file
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
        uri = "wss://data.alpaca.markets/stream"
        async with websockets.connect(uri) as websocket:
            # Authenticate with Alpaca
            await websocket.send(json.dumps({
                "action": "authenticate",
                "data": {
                    "key_id": "YOUR_API_KEY",
                    "secret_key": "YOUR_SECRET_KEY"
                }
            }))
            response = await websocket.recv()
            print(response)  # Handle authentication response

            # Subscribe to the ticker
            await websocket.send(json.dumps({
                "action": "subscribe",
                "trades": [self.ticker_symbol]
            }))
            response = await websocket.recv()
            print(response)  # Handle subscription response

            # Stream data
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(data)  # Process and store data in the database

    def start_streaming(self):
        asyncio.run(self.stream_data())

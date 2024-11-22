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
from hendricks.load_ticker_data import DataLoader

class DataStreamer:
    """
    Stream ticker data from Alpaca API.
    """
    def __init__(self,
                 file: str = None,
                 ticker_symbols: list = None,
                 from_date: str = None,
                 to_date: str = None,
                 collection_name: str = "rawPriceColl",
                 batch_size: int = 7500
                 ):
        self.file = file
        self.ticker_symbols = ticker_symbols
        self.from_date = from_date
        self.to_date = to_date
        self.collection_name = collection_name
        self.batch_size = int(batch_size)
        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")

    async def stream_data(self):
        uri = "wss://stream.data.alpaca.markets/v2/iex"
        data_loader = DataLoader(ticker_symbols=self.ticker_symbols, collection_name=self.collection_name)
        try:
            async with websockets.connect(uri) as websocket:
                # Authenticate with Alpaca
                await websocket.send(json.dumps({
                    "action": "auth",
                    "key": self.API_KEY,
                    "secret": self.API_SECRET
                }))
                response = await websocket.recv()
                print("Authentication response:", response)

                # Subscribe to the ticker
                print(f"Subscribing to {self.ticker_symbols}")
                await websocket.send(json.dumps({
                    "action": "subscribe",
                    "trades": self.ticker_symbols
                }))
                response = await websocket.recv()
                print("Subscription response:", response)

                # Stream data
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    print("Received data:", data)
                    # Process and store the data
                    data_loader.load_stream(data)
                    
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"WebSocket connection closed with error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            print("WebSocket connection closed.")

    def start_streaming(self):
        try:
            # Check if there's an existing event loop
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If no event loop is found, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Run the stream_data coroutine
        loop.run_until_complete(self.stream_data())
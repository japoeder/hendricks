"""
Load ticker data into MongoDB.
"""
import os
import json
import asyncio
import dotenv
import websockets

dotenv.load_dotenv()


class DataStreamer:
    """
    Stream ticker data from Alpaca API.
    """

    def __init__(
        self,
        file: str = None,
        ticker_symbols: list = None,
        collection_name: str = "rawPriceColl",
    ):
        self.file = file
        self.ticker_symbols = ticker_symbols
        self.collection_name = collection_name
        self.API_KEY = os.getenv("API_KEY")
        self.API_SECRET = os.getenv("API_SECRET")

    async def data_stream(self, data_loader):
        """Stream data from the Alpaca API."""
        uri = "wss://stream.data.alpaca.markets/v2/iex"
        while True:  # Loop to handle reconnection
            try:
                async with websockets.connect(uri) as websocket:
                    # Authenticate with Alpaca
                    await websocket.send(
                        json.dumps(
                            {
                                "action": "auth",
                                "key": self.API_KEY,
                                "secret": self.API_SECRET,
                            }
                        )
                    )
                    response = await websocket.recv()
                    print("Authentication response:", response)

                    # Subscribe to the ticker
                    print(f"Subscribing to {self.ticker_symbols}")
                    await websocket.send(
                        json.dumps(
                            {"action": "subscribe", "trades": self.ticker_symbols}
                        )
                    )
                    response = await websocket.recv()
                    print("Subscription response:", response)
                    print(websocket.messages)

                    # Stream data
                    while True:
                        # Wait for messages to be available
                        if (
                            websocket.messages
                        ):  # Check if there are messages in the deque
                            message = (
                                websocket.messages.popleft()
                            )  # Dequeue the first message
                            stream_data = json.loads(message)  # Parse the JSON message
                            # print("Received data:", stream_data)

                            # Process the received data
                            for item in stream_data:
                                # Check if the item is a trade message
                                if (
                                    "T" in item and item["T"] == "t"
                                ):  # 't' indicates a trade message
                                    data_loader.load_stream_doc(item)

                        await asyncio.sleep(0.1)  # Small delay to prevent busy waiting

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"WebSocket connection closed with error: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
            except BrokenPipeError as e:
                print(f"Broken pipe error: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
            except Exception as e:
                print(f"An error occurred: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
            finally:
                print("WebSocket connection closed.")
                # Unsubscribe from the ticker
                try:
                    async with websockets.connect(uri) as websocket:
                        print(f"Unsubscribing from {self.ticker_symbols}")
                        await websocket.send(
                            json.dumps(
                                {"action": "unsubscribe", "trades": self.ticker_symbols}
                            )
                        )
                        response = await websocket.recv()
                        print("Unsubscription response:", response)
                except Exception as e:
                    print(f"An error occurred during unsubscription: {e}")
                print("WebSocket connection closed.")

    def start_streaming(self, data_loader):
        """Start the streaming process."""
        try:
            # Check if there's an existing event loop
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If no event loop is found, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Run the stream_data coroutine
        loop.run_until_complete(self.data_stream(data_loader))

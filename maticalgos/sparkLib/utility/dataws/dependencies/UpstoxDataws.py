from typing import Optional, Callable
import asyncio
import json
import ssl
import requests
import websockets
from google.protobuf.json_format import MessageToDict
from . import MarketDataFeedUps as pb

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


class AsyncUpstockOrderUpdate:
    Authorize_URL = 'https://api.upstox.com/v2/feed/market-data-feed/authorize'
    Api_Version = '2.0'

    def __init__(
            self,
            auth_token: str,
            on_message: Optional[Callable] = None,
            on_error: Optional[Callable] = None,
            on_connect: Optional[Callable] = None,
            on_close: Optional[Callable] = None,
            reconnect_retry: int = 5,
    ):
        self.auth_token = f'Bearer {auth_token}'
        self.onmessage = on_message
        self.onerror = on_error
        self.onconnect = on_connect
        self.onclose = on_close
        self.max_retry_attempts = reconnect_retry
        self.current_retry_attempt = 0
        self.websocket = None
        self.running = True  # Flag to control running state

    def getWSUrl(self):
        headers = {
            "Authorization": self.auth_token,
            "Api-Version": self.Api_Version,
            "Accept": "application/json"
        }
        response = requests.get(self.Authorize_URL, headers=headers)
        if response.status_code == 200:
            return response.json().get('data').get('authorized_redirect_uri')
        else:
            return None


    async def connect(self):
        """Connect to the WebSocket and handle messages asynchronously."""
        try:

            # Extract WebSocket URL
            ws_url = self.getWSUrl()
            # Set up SSL context
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                self.websocket = websocket
                if self.onconnect:
                    await self.onconnect()
                self.current_retry_attempt = 0
                # crate a new task to listen for messages
                asyncio.create_task(self.listen())

                await asyncio.Future()
        except Exception as e:
            if self.onerror:
                await self.onerror(e)
            await self.retry_connect()

    async def retry_connect(self):
        """Attempt to reconnect with exponential backoff."""
        if self.current_retry_attempt < self.max_retry_attempts and self.running:
            retry_delay = 2 ** self.current_retry_attempt  # Exponential backoff
            print(f"Retrying connection in {retry_delay} seconds ", f"({self.current_retry_attempt + 1}/{self.max_retry_attempts})")
            await asyncio.sleep(retry_delay)
            self.current_retry_attempt += 1
            await self.connect()
        else:
            self.running = False
            await self.onclose(1006, "Connection closed due to max retry attempts")

    async def listen(self):
        """Listen for messages from the WebSocket."""
        try:
            while self.running:
                message = await self.websocket.recv()
                decoded_data = decode_protobuf(message)
                data_dict = MessageToDict(decoded_data)

                if self.onmessage:
                    await self.onmessage(data_dict)

        except websockets.ConnectionClosed as e:
            if self.onclose:
                await self.onclose(e.code, e.reason)
            await self.retry_connect()
        except Exception as e:
            if self.onerror:
                await self.onerror(e)

    def subscribe(self, mode: str, instrument_keys: list):
        """Send a subscription message to the WebSocket."""
        try:
            data = {
                "guid": "someguid",
                "method": "sub",
                "data": {
                    "mode": mode,
                    "instrumentKeys": instrument_keys
                }
            }
            binary_data = json.dumps(data).encode('utf-8')
            asyncio.create_task(self.websocket.send(binary_data))
            # self.websocket.send(binary_data)
        except Exception as e:
            print(f"Error in subscribing: {e}")


    def unsubscribe(self, mode: str, instrument_keys: list):
        """Send an unsubscription message to the WebSocket."""
        try:
            data = {
                "guid": "someguid",
                "method": "unsub",
                "data": {
                    "mode": mode,
                    "instrumentKeys": instrument_keys
                }
            }
            binary_data = json.dumps(data).encode('utf-8')
            # self.websocket.send(binary_data)
            asyncio.create_task(self.websocket.send(binary_data))
        except Exception as e:
            print(f"Error in unsubscribing: {e}")

    async def close_connection(self):
        """Close the WebSocket connection."""
        self.running = False  # Stop listening loop
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
        else:
            print("Connection already closed.")


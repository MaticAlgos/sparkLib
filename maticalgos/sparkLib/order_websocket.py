from typing import Any, Callable, Optional
import websocket
import time
import threading

class OrderSocket:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self,
            access_token: str,
            on_order: Optional[Callable] = None,
            on_error: Optional[Callable] = None,
            on_connect: Optional[Callable] = None,
            on_close: Optional[Callable] = None,
            run_background: bool = False,
            reconnect: bool = True,
            max_reconnect_attempts: int = 50,
            reconnect_delay: int = 2
    ) -> None:
        self.__access_token = access_token
        self.__url = "wss://apiv.maticalgos.com/orderWS"
        self.__ws_object = None

        self.onorder = on_order
        self.onerror = on_error
        self.onconnect = on_connect
        self.onclose = on_close

        self.run_background = run_background
        self.reconnect = reconnect
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay

    def on_order_callback(self,message):
        # print("on_order_callback called",locals())
        try:
            if self.onorder is not None:
                self.onorder(message)
            else:
                print(f"Order : {message}")
        except Exception as e:
            print("Error in on_order", e)

    def on_error_callback(self,error):
        # print("on_error_callback called",locals())
        try:
            if self.onerror is not None:
                self.onerror(error)
            else:
                print(f"Error : {error}")
            self.__on_close()
        except Exception as e:
            print("Error in on_error", e)

    def on_open(self):
        try:
            if self.onconnect is not None:
                self.onconnect()
            else:
                print("Connected")
        except Exception as e:
            print("Error in on_open", e)

    def on_close(self):
        try:
            if self.onclose is not None:
                self.onclose()
            else:
                print("Connection Closed")
        except Exception as e:
            print("Error in on_close", e)

    def __on_close(self):
        if self.reconnect:
            if self.reconnect_attempts < self.max_reconnect_attempts:
                print(f"Reconnecting in {self.reconnect_delay} seconds - Attempt {self.reconnect_attempts} of {self.max_reconnect_attempts}")
                self.reconnect_attempts += 1
                time.sleep(self.reconnect_delay)
                self.__ws_object.close()
                self.__ws_object.run_forever(ping_interval=2, ping_timeout=1)
            else:
                print("Max Reconnect Attempts reached")
                self.close_connection()
                self.on_close()
                

    def connect(self):
        # print("Connecting to WebSocket",locals())
        self.__url = f"{self.__url}?token={self.__access_token}"
        self.__ws_object = websocket.WebSocketApp(
            url=self.__url
            , on_open= self.on_open()
            , on_message=lambda ws, message: self.on_order_callback(message)
            , on_error=lambda ws, error: self.on_error_callback(error)
            , on_close=lambda ws, close_code, close_reason: self.__on_close()
        )
        # self.__ws_object.run_forever(ping_interval=2, ping_timeout=1)
        self.t = threading.Thread(target=self.__ws_object.run_forever, kwargs={"ping_interval": 2, "ping_timeout": 1})
        self.t.daemon = not self.run_background
        self.t.start()

    def close_connection(self):
        self.reconnect = False
        if self.t and self.t.is_alive():
            self.__ws_object.close()
#            self.t.join()  # Wait for the thread to terminate
            print("WebSocket connection closed.")
        else:
            print("WebSocket connection thread is not active.")

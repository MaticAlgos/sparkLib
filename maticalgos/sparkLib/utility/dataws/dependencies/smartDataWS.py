import struct
import time
import ssl
import json
import websocket
import os
from threading import Thread

class SmartWebSocketV2(object):
    """
    SmartAPI Web Socket version 2
    """

    ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream"
    HEART_BEAT_MESSAGE = "ping"
    HEART_BEAT_INTERVAL = 10  # Adjusted to 10s
    LITTLE_ENDIAN_BYTE_ORDER = "<"
    # HB_THREAD_FLAG = True

    # Available Actions
    SUBSCRIBE_ACTION = 1
    UNSUBSCRIBE_ACTION = 0

    # Possible Subscription Mode
    LTP_MODE = 1
    QUOTE = 2
    SNAP_QUOTE = 3
    DEPTH = 4

    # Exchange Type
    NSE_CM = 1
    NSE_FO = 2
    BSE_CM = 3
    BSE_FO = 4
    MCX_FO = 5
    NCX_FO = 7
    CDE_FO = 13

    # Subscription Mode Map
    SUBSCRIPTION_MODE_MAP = {
        1: "LTP",
        2: "QUOTE",
        3: "SNAP_QUOTE",
        4: "DEPTH"
    }

    wsapp = None
    input_request_dict = {}
    current_retry_attempt = 0

    def __init__(self, auth_token, api_key, client_code, feed_token, max_retry_attempt=1,
                 on_Message=None, on_Error=None, on_Close=None, on_Open=None
                 ):
        """
            Initialise the SmartWebSocketV2 instance
            Parameters
            ------
            auth_token: string
                jwt auth token received from Login API
            api_key: string
                api key from Smart API account
            client_code: string
                angel one account id
            feed_token: string
                feed token received from Login API
        """
        self.auth_token = auth_token
        self.api_key = api_key
        self.client_code = client_code
        self.feed_token = feed_token
        self.last_pong_timestamp = None
        self.MAX_RETRY_ATTEMPT = max_retry_attempt
        self.retry_delay = 10
        self.retry_duration = 60

        self.on_data = on_Message
        self.on_error = on_Error
        self.on_close = on_Close
        self.on_open = on_Open

        if not self._sanity_check():
            raise Exception("Provide valid value for all the tokens")

    def _sanity_check(self):
        if not all([self.auth_token, self.api_key, self.client_code, self.feed_token]):
            return False
        return True

    def _on_message(self, wsapp, message):
        if message != "pong":
            parsed_message = self._parse_binary_data(message)
            # Check if it's a control message (e.g., heartbeat)
            if self._is_control_message(parsed_message):
                self._handle_control_message(parsed_message)
            else:
                self.on_data(parsed_message)
        else:
            self.on_data(message)

    def _is_control_message(self, parsed_message):
        return "subscription_mode" not in parsed_message

    def _handle_control_message(self, parsed_message):
        if parsed_message["subscription_mode"] == 0:
            self._on_pong(self.wsapp, "pong")
        elif parsed_message["subscription_mode"] == 1:
            self._on_ping(self.wsapp, "ping")
        # Invoke on_control_message callback with the control message data
        if hasattr(self, 'on_control_message'):
            self.on_control_message(self.wsapp, parsed_message)

    def _on_data(self, wsapp, data, data_type, continue_flag):
        if data_type == 2:
            parsed_message = self._parse_binary_data(data)
            self.on_data(parsed_message)

    def _on_open(self, wsapp):
        try:
            if self.on_open:
                self.on_open()
        except Exception as e:
            print("Error in on_open function: %s", e)
        self.current_retry_attempt = 0


    def subscribe(self, correlation_id, mode, token_list):
        """
            This Function subscribe the price data for the given token
            Parameters
            ------
            correlation_id: string
                A 10 character alphanumeric ID client may provide which will be returned by the server in error response
                to indicate which request generated error response.
                Clients can use this optional ID for tracking purposes between request and corresponding error response.
            mode: integer
                It denotes the subscription type
                possible values -> 1, 2 and 3
                1 -> LTP
                2 -> Quote
                3 -> Snap Quote
            token_list: list of dict
                Sample Value ->
                    [
                        { "exchangeType": 1, "tokens": ["10626", "5290"]},
                        {"exchangeType": 5, "tokens": [ "234230", "234235", "234219"]}
                    ]
                    exchangeType: integer
                    possible values ->
                        1 -> nse_cm
                        2 -> nse_fo
                        3 -> bse_cm
                        4 -> bse_fo
                        5 -> mcx_fo
                        7 -> ncx_fo
                        13 -> cde_fo
                    tokens: list of string
        """
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.SUBSCRIBE_ACTION,
                "params": {
                    "mode": mode,
                    "tokenList": token_list
                }
            }
            if mode == 4:
                for token in token_list:
                    if token.get('exchangeType') != 1:
                        error_message = f"Invalid ExchangeType:{token.get('exchangeType')} Please check the exchange type and try again it support only 1 exchange type"
                        raise ValueError(error_message)

            if self.input_request_dict.get(mode) is None:
                self.input_request_dict[mode] = {}

            for token in token_list:
                if token['exchangeType'] in self.input_request_dict[mode]:
                    self.input_request_dict[mode][token['exchangeType']].extend(token["tokens"])
                else:
                    self.input_request_dict[mode][token['exchangeType']] = token["tokens"]

            if mode == self.DEPTH:
                total_tokens = sum(len(token["tokens"]) for token in token_list)
                quota_limit = 50
                if total_tokens > quota_limit:
                    error_message = f"Quota exceeded: You can subscribe to a maximum of {quota_limit} tokens only."
                    raise Exception(error_message)

            self.wsapp.send(json.dumps(request_data))

        except Exception as e:
            raise e

    def unsubscribe(self, correlation_id, mode, token_list):
        """
            This function unsubscribe the data for given token
            Parameters
            ------
            correlation_id: string
                A 10 character alphanumeric ID client may provide which will be returned by the server in error response
                to indicate which request generated error response.
                Clients can use this optional ID for tracking purposes between request and corresponding error response.
            mode: integer
                It denotes the subscription type
                possible values -> 1, 2 and 3
                1 -> LTP
                2 -> Quote
                3 -> Snap Quote
            token_list: list of dict
                Sample Value ->
                    [
                        { "exchangeType": 1, "tokens": ["10626", "5290"]},
                        {"exchangeType": 5, "tokens": [ "234230", "234235", "234219"]}
                    ]
                    exchangeType: integer
                    possible values ->
                        1 -> nse_cm
                        2 -> nse_fo
                        3 -> bse_cm
                        4 -> bse_fo
                        5 -> mcx_fo
                        7 -> ncx_fo
                        13 -> cde_fo
                    tokens: list of string
        """
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.UNSUBSCRIBE_ACTION,
                "params": {
                    "mode": mode,
                    "tokenList": token_list
                }
            }
            self.input_request_dict.update(request_data)
            self.wsapp.send(json.dumps(request_data))
        except Exception as e:
            raise e

    def wsapp_run_forever(self):
        self.wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.HEART_BEAT_INTERVAL,
                               ping_payload=self.HEART_BEAT_MESSAGE)#,ping_timeout=self.HEART_BEAT_INTERVAL-1)

    def connect(self):
        """
            Make the web socket connection with the server
        """
        headers = {
            "Authorization": self.auth_token,
            "x-api-key": self.api_key,
            "x-client-code": self.client_code,
            "x-feed-token": self.feed_token
        }

        try:
            self.wsapp = websocket.WebSocketApp(self.ROOT_URI, header=headers, on_open=self._on_open,
                                                on_error=self._on_error, on_close=self._on_close, on_data=self._on_data,
                                                )

            self.ws_thread = Thread(target=self.wsapp_run_forever)
            self.ws_thread.daemon = False
            self.ws_thread.start()
        except Exception as e:
            print(f"Error occurred during WebSocket connection: {e}")
            raise e

    def close_connection(self):
        """
        Closes the connection
        """
        if self.wsapp:
            self.wsapp.close()

    def _on_error(self, wsapp, error):
        try:
            if self.on_error:
                self.on_error(error)
        except Exception as e:
            print("Error in on_error function: %s", e)
        self.retry_connect()

    def retry_connect(self):
        if self.current_retry_attempt < self.MAX_RETRY_ATTEMPT:
            print(f"Retrying connection for {self.current_retry_attempt+1} of {self.MAX_RETRY_ATTEMPT}")
            time.sleep(self.retry_delay)
            self.current_retry_attempt += 1
            self.connect()
        else:
            print("Max retry attempts reached.")

    def _on_close(self, wsapp, close_status_code, close_msg):
        self.on_close(close_status_code, close_msg)

    def _parse_binary_data(self, binary_data):
        parsed_data = {
            "subscription_mode": self._unpack_data(binary_data, 0, 1, byte_format="B")[0],
            "exchange_type": self._unpack_data(binary_data, 1, 2, byte_format="B")[0],
            "token": SmartWebSocketV2._parse_token_value(binary_data[2:27]),
            "sequence_number": self._unpack_data(binary_data, 27, 35, byte_format="q")[0],
            "exchange_timestamp": self._unpack_data(binary_data, 35, 43, byte_format="q")[0],
            "last_traded_price": self._unpack_data(binary_data, 43, 51, byte_format="q")[0]
        }
        try:
            parsed_data["subscription_mode_val"] = self.SUBSCRIPTION_MODE_MAP.get(parsed_data["subscription_mode"])

            if parsed_data["subscription_mode"] in [self.QUOTE, self.SNAP_QUOTE]:
                parsed_data["last_traded_quantity"] = self._unpack_data(binary_data, 51, 59, byte_format="q")[0]
                parsed_data["average_traded_price"] = self._unpack_data(binary_data, 59, 67, byte_format="q")[0]
                parsed_data["volume_trade_for_the_day"] = self._unpack_data(binary_data, 67, 75, byte_format="q")[0]
                parsed_data["total_buy_quantity"] = self._unpack_data(binary_data, 75, 83, byte_format="d")[0]
                parsed_data["total_sell_quantity"] = self._unpack_data(binary_data, 83, 91, byte_format="d")[0]
                parsed_data["open_price_of_the_day"] = self._unpack_data(binary_data, 91, 99, byte_format="q")[0]
                parsed_data["high_price_of_the_day"] = self._unpack_data(binary_data, 99, 107, byte_format="q")[0]
                parsed_data["low_price_of_the_day"] = self._unpack_data(binary_data, 107, 115, byte_format="q")[0]
                parsed_data["closed_price"] = self._unpack_data(binary_data, 115, 123, byte_format="q")[0]

            if parsed_data["subscription_mode"] == self.SNAP_QUOTE:
                parsed_data["last_traded_timestamp"] = self._unpack_data(binary_data, 123, 131, byte_format="q")[0]
                parsed_data["open_interest"] = self._unpack_data(binary_data, 131, 139, byte_format="q")[0]
                parsed_data["open_interest_change_percentage"] = self._unpack_data(binary_data, 139, 147, byte_format="q")[0]
                parsed_data["upper_circuit_limit"] = self._unpack_data(binary_data, 347, 355, byte_format="q")[0]
                parsed_data["lower_circuit_limit"] = self._unpack_data(binary_data, 355, 363, byte_format="q")[0]
                parsed_data["52_week_high_price"] = self._unpack_data(binary_data, 363, 371, byte_format="q")[0]
                parsed_data["52_week_low_price"] = self._unpack_data(binary_data, 371, 379, byte_format="q")[0]
                best_5_buy_and_sell_data = self._parse_best_5_buy_and_sell_data(binary_data[147:347])
                parsed_data["best_5_buy_data"] = best_5_buy_and_sell_data["best_5_sell_data"]
                parsed_data["best_5_sell_data"] = best_5_buy_and_sell_data["best_5_buy_data"]

            if parsed_data["subscription_mode"] == self.DEPTH:
                parsed_data.pop("sequence_number", None)
                parsed_data.pop("last_traded_price", None)
                parsed_data.pop("subscription_mode_val", None)
                parsed_data["packet_received_time"]=self._unpack_data(binary_data, 35, 43, byte_format="q")[0]
                depth_data_start_index = 43
                depth_20_data = self._parse_depth_20_buy_and_sell_data(binary_data[depth_data_start_index:])
                parsed_data["depth_20_buy_data"] = depth_20_data["depth_20_buy_data"]
                parsed_data["depth_20_sell_data"] = depth_20_data["depth_20_sell_data"]

            return parsed_data
        except Exception as e:
            print(f"Error occurred during binary data parsing: {e}")
            raise e

    def _unpack_data(self, binary_data, start, end, byte_format="I"):
        """
            Unpack Binary Data to the integer according to the specified byte_format.
            This function returns the tuple
        """
        return struct.unpack(self.LITTLE_ENDIAN_BYTE_ORDER + byte_format, binary_data[start:end])

    @staticmethod
    def _parse_token_value(binary_packet):
        token = ""
        for i in range(len(binary_packet)):
            if chr(binary_packet[i]) == '\x00':
                return token
            token += chr(binary_packet[i])
        return token

    def _parse_best_5_buy_and_sell_data(self, binary_data):

        def split_packets(binary_packets):
            packets = []

            i = 0
            while i < len(binary_packets):
                packets.append(binary_packets[i: i + 20])
                i += 20
            return packets

        best_5_buy_sell_packets = split_packets(binary_data)

        best_5_buy_data = []
        best_5_sell_data = []

        for packet in best_5_buy_sell_packets:
            each_data = {
                "flag": self._unpack_data(packet, 0, 2, byte_format="H")[0],
                "quantity": self._unpack_data(packet, 2, 10, byte_format="q")[0],
                "price": self._unpack_data(packet, 10, 18, byte_format="q")[0],
                "no of orders": self._unpack_data(packet, 18, 20, byte_format="H")[0]
            }

            if each_data["flag"] == 0:
                best_5_buy_data.append(each_data)
            else:
                best_5_sell_data.append(each_data)

        return {
            "best_5_buy_data": best_5_buy_data,
            "best_5_sell_data": best_5_sell_data
        }

    def _parse_depth_20_buy_and_sell_data(self, binary_data):
        depth_20_buy_data = []
        depth_20_sell_data = []

        for i in range(20):
            buy_start_idx = i * 10
            sell_start_idx = 200 + i * 10

            # Parse buy data
            buy_packet_data = {
                "quantity": self._unpack_data(binary_data, buy_start_idx, buy_start_idx + 4, byte_format="i")[0],
                "price": self._unpack_data(binary_data, buy_start_idx + 4, buy_start_idx + 8, byte_format="i")[0],
                "num_of_orders": self._unpack_data(binary_data, buy_start_idx + 8, buy_start_idx + 10, byte_format="h")[0],
            }

            # Parse sell data
            sell_packet_data = {
                "quantity": self._unpack_data(binary_data, sell_start_idx, sell_start_idx + 4, byte_format="i")[0],
                "price": self._unpack_data(binary_data, sell_start_idx + 4, sell_start_idx + 8, byte_format="i")[0],
                "num_of_orders": self._unpack_data(binary_data, sell_start_idx + 8, sell_start_idx + 10, byte_format="h")[0],
            }

            depth_20_buy_data.append(buy_packet_data)
            depth_20_sell_data.append(sell_packet_data)

        return {
            "depth_20_buy_data": depth_20_buy_data,
            "depth_20_sell_data": depth_20_sell_data
        }

    # def on_message(self, wsapp, message):
    #     print('Received message: ', message)
    #
    # def on_data(self, wsapp, data):
    #     print('Received data: ', data)
    #
    # def on_control_message(self, wsapp, message):
    #     print('Received control message: ', message)
    #
    # def on_close(self, wsapp, close_status_code, close_msg):
    #     print('Connection closed')
    #
    # def on_open(self, wsapp):
    #     print('Connection opened')
    #
    # def on_error(self, error):
    #     print('Error occurred')

if __name__ == "__main__":
    AUTH_TOKEN = 'eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Ik4yNzg3MTgiLCJyb2xlcyI6MCwidXNlcnR5cGUiOiJVU0VSIiwidG9rZW4iOiJleUpoYkdjaU9pSlNVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlYM1I1Y0dVaU9pSmpiR2xsYm5RaUxDSjBiMnRsYmw5MGVYQmxJam9pZEhKaFpHVmZZV05qWlhOelgzUnZhMlZ1SWl3aVoyMWZhV1FpT2pRc0luTnZkWEpqWlNJNklqTWlMQ0prWlhacFkyVmZhV1FpT2lJd1pXSXlPR05tWWkweFpqSXdMVE13WW1ZdFltSXpNeTB6T1RrMll6azVPRFV6TldVaUxDSnJhV1FpT2lKMGNtRmtaVjlyWlhsZmRqRWlMQ0p2Ylc1bGJXRnVZV2RsY21sa0lqbzBMQ0p3Y205a2RXTjBjeUk2ZXlKa1pXMWhkQ0k2ZXlKemRHRjBkWE1pT2lKaFkzUnBkbVVpZlgwc0ltbHpjeUk2SW5SeVlXUmxYMnh2WjJsdVgzTmxjblpwWTJVaUxDSnpkV0lpT2lKT01qYzROekU0SWl3aVpYaHdJam94TnpFNU5EY3pOakUxTENKdVltWWlPakUzTVRrek56WTVNamNzSW1saGRDSTZNVGN4T1RNM05qa3lOeXdpYW5ScElqb2lORGhoWkRjM01EVXRabU5rTWkwMFlqQTJMVGxsTm1VdE1UUmlaRFV6TmpKallXRmhJbjAuU2RJUFI4OWk3VnQ0UlA0TExFelpSaTdSMG94VnlzVHhXbjlXeW5mQUhUaTZ0OVVzdjNrV2VVNkVubUpxWWpLdXVCaklfbVg3clM5aThWNk5jdWFOMHVESm85SVJCRVlZRWJJZDVfZmtWay1STU1MbWhrN093UWVMdk90QnJvRllhaGVDRG5NbjR1UTJzQ3Z4Z1VWZUJkTTNTeFY2dUxXd2JSdlFGa01nMzRBIiwiQVBJLUtFWSI6IlZhQXJkak1CIiwiaWF0IjoxNzE5Mzc2OTg3LCJleHAiOjE3MTk0NzM2MTV9.JjHFs-ILCP0ZFUsYewNdufrqokLXemawEjhWMU4utqDa9PboiaWsySfYC32qKL6vnSUzyTZavh_wpPtBDe4q4Q'
    API_KEY = 'VaArdjMB'
    CLIENT_CODE = 'N278718'
    FEED_TOKEN    = 'eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Ik4yNzg3MTgiLCJpYXQiOjE3MTkzNzY5ODcsImV4cCI6MTcxOTQ2MzM4N30.76Z3LZmCmB4IdVO8uL3Qw8_2H2zlvIc7x-uR4Lqc08rBjXh2i_yTOZZinNlWy4QlWDIpYOp5_OSKAcvSzjXb9A'
    correlation_id = "abc123"
    action = 1
    # ACTION:
        # 1 : subscribe
        # 0 : unsubscribe
        
    mode = 1
    # modes : 
    # 1 (LTP)
    # 2 (Quote)
    # 3 (Snap Quote)
    # 4 (20-Depth)
    
    token_list = [
        {
            "exchangeType": 1,
            "tokens": ["26009","1594"]
        }
    ]
    token_list1 = [
        {
            "action": 0,
            "exchangeType": 1,
            "tokens": ["26009"]
        }
    ]
    
    def on_message(message):
        print("Ticks: {}".format(message))

    def on_open():
        print("on open")
        sws.subscribe(correlation_id, mode, token_list)
        # sws.unsubscribe(correlation_id, mode, token_list1)

    def on_error(error):
        print(f'Error : {error}')

    def on_close(close_status_code, close_msg):
        print(f"Closed : {close_status_code} : {close_msg}")

    sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN,max_retry_attempt=3,
                           on_Message=on_message, on_Error=on_error, on_Close=on_close, on_Open=on_open)

    sws.connect()
    time.sleep(40)
    sws.close_connection()


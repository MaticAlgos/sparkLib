import base64
import logging
import threading
import time
from typing import Optional, Callable

from pkg_resources import resource_filename
import requests
import urllib.parse
import websocket
from threading import Thread
import struct
import json
import os 

# from fyers_apiv3.FyersWebsocket import defines
# from fyers_apiv3.fyers_logger import FyersLogger
from . import defines
from .fyers_logger import FyersLogger 

class SymbolConversion:
    def __init__(self, access_token: str, data_type: str, log_path: str):
        """
        Initializes a SymbolConversion instance.

        Args:
            access_token (str): The access token used for authentication.
            data_type (str): The data_type associated with the symbol conversion. 
                            Valid values are 'Symbolupdate' or 'DepthUpdate'.

        """
        self.data_type = data_type
        if ":" in access_token:
            access_token = access_token.split(":")[1]
        self.access_token = access_token
        self.log_path = log_path
        self.symbols_token_api = "https://api-t1.fyers.in/data/symbol-token"

        if log_path:
            self.log_path = log_path + "/"
        else:
            self.log_path = ""
        self.data_logger = FyersLogger(
            "FyersDataSocket",
            "DEBUG",
            stack_level=3,
            logger_handler=logging.FileHandler(log_path + "fyersDataSocket.log"),
        )

    def symbol_to_hsmtoken(self, symbols: list):
        """
        Converts symbols to HSM tokens.

        Args:
            symbols (list): A list of symbols to be converted.

        Returns:
            tuple: A tuple containing dictionary and list.
                - The first dictionary represents the mapping of symbols to HSM tokens.
                - The second list represents any symbols that could not be converted.

        """
        try:
            data = {"symbols": symbols}
            response = requests.post(
                url=self.symbols_token_api ,
                headers={
                    "Authorization": self.access_token,
                    "Content-Type": "application/json",
                },json=data
            )
            response_data =response.json()
            datadict = {}
            project_path =  os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            file_path = os.path.join(project_path, 'dependencies', 'fyersAPIv3', 'map.json')
            # file_path = resource_filename('fyers_apiv3.FyersWebsocket', 'map.json')
            with open(file_path, "r") as file:
                mapper = json.load(file)
            index_dict = mapper["index_dict"]
            exch_seg_dict = mapper["exch_seg_dict"]
            wrong_symbol = []
            dp_index_flag = False

            if response_data['s'] == "ok":
                for symbol, fytoken in response_data["validSymbol"].items():
                    ex_sg = fytoken[:4]
                    if ex_sg not in exch_seg_dict:
                        continue
                    segment = exch_seg_dict[ex_sg]
                    symbol_split = symbol.split("-")
                    update_dict = True
                    if len(symbol_split) > 1 and symbol_split[-1] == "INDEX" and self.data_type != "DepthUpdate":
                        if symbol in index_dict:
                                exch_token = index_dict[symbol]
                        else:
                            exch_token = (
                                symbol.split(":")[1].split("-")[0]
                            )
                        hsm_symbol = (
                                "if" + "|" + segment + "|" + exch_token
                            )                
                    elif self.data_type == "DepthUpdate" and symbol_split[-1] != "INDEX":
                        
                        exch_token = fytoken[10:]
                        hsm_symbol = (
                            "dp" + "|" + segment + "|" + exch_token
                        )
                    elif self.data_type == "SymbolUpdate":
                        exch_token = fytoken[10:]
                        hsm_symbol = (
                            "sf" + "|" + segment + "|" + exch_token
                        )                        
                    elif self.data_type == "DepthUpdate" and symbol_split[-1] == "INDEX":
                        update_dict = False
                        dp_index_flag =True

                    if update_dict:
                        datadict[hsm_symbol] = symbol
                if response_data["invalidSymbol"]:
                    wrong_symbol = response_data["invalidSymbol"]
                return (datadict, wrong_symbol, dp_index_flag,"")

            elif response_data['s'] == "error":
                
                return ({}, [],dp_index_flag, response_data["message"])


        except Exception as e:
            self.data_logger.exception(e)


class FyersDataSocket:

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        access_token: str,
        write_to_file: bool = False,
        log_path: Optional[str] = None,
        litemode: bool = False,
        reconnect: bool = True,
        on_message: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
        on_connect: Optional[Callable] = None,
        on_close: Optional[Callable] = None,
        reconnect_retry: int = 5
    ):

        """
        Initialize YourClass instance.

        Args:
            access_token (str): The access token used for authentication.
            write_to_file (bool, optional): Specifies if the class should run in the background.
                Defaults to False.
            log_path (str, optional): The path to the log file. Defaults to None.
            litemode (bool, optional): Specifies if the class should run in litemode.
                Defaults to False.
            reconnect (bool, optional): Specifies if the class should attempt to reconnect on failure.
                Defaults to True.
            on_message (callable, optional): Callback function to be executed when a message is received.
                Defaults to None.
            on_error (callable, optional): Callback function to be executed when an error occurs.
                Defaults to None.
            on_connect (callable, optional): Callback function to be executed when a connection is established.
                Defaults to None.
            on_close (callable, optional): Callback function to be executed when a connection is closed.
                Defaults to None.
        """
        
        self.__url = "wss://socket.fyers.in/hsm/v1-5/prod"
        self.__access_token = access_token
        self.__hsm_token = ""
        self.log_path = log_path
        self.lite = litemode
        self.max_retry = reconnect_retry
        self.source = "PythonSDK-3.0.9"
        self.channel_num = 11
        self.channels = []
        self.running_channels = set()
        self.data_type = None
        self.OnMessage = on_message
        self.OnError = on_error
        self.OnOpen = on_connect
        self.OnClose = on_close
        self.UpdateTick = False
        self.ack_count = 0
        self.__ws_run = None
        self.write_to_file = write_to_file
        self.background_flag = False
        self.update_count = 0
        self.literesp = {}
        self.channel_symbol = []
        self.symbol_dict = {}
        self.scrips_count = {}
        self.scrips_per_channel = {}
        self.restart_flag = reconnect
        self.websocket_lock = threading.Lock()
        self.message_lock = threading.Lock()
        self.message_condition = threading.Condition(lock=self.message_lock)
        self.unsub_symbol = []
        for i in range(1, 31):
            self.scrips_per_channel[i] = []
        self.active_channel = None
        self.message = []
        self.resp = {}
        self.__ws_object = None
        self.__valid_token = False
        self.dp_sym = {}
        self.ping_thread = None
        self.message_thread = None
        self.message_thread_stop_event = None
        self.ws_thread = None
        self.infy_loop = None
        self.symbol_limit = 5000
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 50
        if reconnect_retry < self.max_reconnect_attempts:
            self.max_reconnect_attempts = reconnect_retry
       
        self.mode = "P"
        self.reconnect_delay = 0
        self.index_sym = {}
        self.scrips_sym = {}
        self.symbol_token = {}
        self.ack_bool = False     
        if log_path:
            self.log_path = log_path + "/"
        else:
            self.log_path = ""
        self.data_logger = FyersLogger(
            "FyersDataSocket",
            "DEBUG",
            stack_level=3,
            logger_handler=logging.FileHandler(self.log_path + "fyersDataSocket.log"),
        )
        project_path =  os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        file_path = os.path.join(project_path, 'dependencies', 'fyersAPIv3', 'map.json')

        with open(file_path, "r") as file:
            # Imported json file
            mapper = json.load(file)

        self.data_val = mapper["data_val"]
        self.index_val = mapper["index_val"]
        self.lite_val = mapper["lite_val"]
        self.depthvalue = mapper["depthvalue"]

    def access_token_to_hsmtoken(self) -> bool :
        """
        Decode APIv2 token to extract 'hsm_key' and check for validity.

        This function decodes the APIv2 access token and extracts the 'hsm_key' from it.
        It also verifies if the token is expired by comparing the 'exp' (expiration) claim in the
        token's payload with the current timestamp. If the token is valid and not expired, it sets
        the 'hsm_token' attribute and returns True. Otherwise, it raises an error and returns False.

        Returns:
            bool: True if the token is valid and not expired, False otherwise.
        """
        try:
            header_token , payload_b64, _ = self.__access_token.split(".")
            # Decode the base64 encoded payload
            decoded_header = base64.urlsafe_b64decode(header_token + "===")
            decoded_payload = base64.urlsafe_b64decode(payload_b64 + "===")
            # Convert the decoded payload to a string (assuming it's in JSON format)
            decode_token = json.loads(decoded_payload.decode())
            today = int(time.time())
            if decode_token["exp"] - today  < 0:
                self.On_error(
                    {   
                        "type": defines.AUTH_TYPE,
                        "code": defines.TOKEN_EXPIRED,
                        "message": defines.TOKEN_EXPIRED_MSG,
                        "s": defines.ERROR,
                    }
                )
                return False
            self.__valid_token = True

            self.__hsm_token = decode_token["hsm_key"]
            return True
        
        except:
            self.On_error(
                {   "type": defines.AUTH_TYPE,
                    "code": defines.INVALID_CODE,
                    "message": defines.INVALID_TOKEN,
                    "s": defines.ERROR,
                }
            )
            # self.data_logger.error(e)
            return False



    def __access_token_msg(self) -> bytearray:
        """
        Create a message in bytearray for token.

        Returns:
            bytearray: The token message in bytearray format.
        """
        try:
            buffer_size = 18 + len(self.__hsm_token) + len(self.source)

            # Create the byte buffer
            byte_buffer = bytearray()

            # Pack data length into the byte buffer
            byte_buffer.extend(struct.pack("!H", buffer_size - 2))

            # Set ReqType
            byte_buffer.extend(bytes([1]))

            # Set FieldCount
            byte_buffer.extend(bytes([4]))

            # Field-1: AuthToken
            field1_id = 1
            field1_size = len(self.__hsm_token)
            byte_buffer.extend(bytes([field1_id]))
            byte_buffer.extend(struct.pack("!H", field1_size))
            byte_buffer.extend(self.__hsm_token.encode())

            # Field-2
            field2_id = 2
            field2_size = 1
            byte_buffer.extend(bytes([field2_id]))
            byte_buffer.extend(struct.pack("!H", field2_size))
            byte_buffer.extend(self.mode.encode('utf-8'))

            # Field-3
            field3_id = 3
            field3_size = 1
            byte_buffer.extend(bytes([field3_id]))
            byte_buffer.extend(struct.pack("!H", field3_size))
            byte_buffer.extend(bytes([1]))

            # Field-4: self.source
            field4_id = 4
            field4_size = len(self.source)
            byte_buffer.extend(bytes([field4_id]))
            byte_buffer.extend(struct.pack("!H", field4_size))
            byte_buffer.extend(self.source.encode())

            return byte_buffer

        except Exception as e:
            self.data_logger.exception(e)

    def __lite_mode_msg(self) -> bytearray:
        """
        Create a message in bytearray for lite mode connection.

        Returns:
            bytearray: The lite mode message in bytearray format.
        """

        try:
            self.channels = [self.channel_num]
            data = bytearray()

            data.extend(struct.pack(">H", 0))

            data.extend(struct.pack("B", 12))

            data.extend(struct.pack("B", 2))

            channel_bits = 0
            for channel_num in self.channels:
                if channel_num < 64 and channel_num > 0:
                    channel_bits |= 1 << channel_num
            # Field-1
            field_1 = bytearray()
            field_1.extend(struct.pack("B", 1))
            field_1.extend(struct.pack(">H", 8))
            field_1.extend(struct.pack(">Q", channel_bits))
            data.extend(field_1)

            # Field-2
            field_2 = bytearray()
            field_2.extend(struct.pack("B", 2))
            field_2.extend(struct.pack(">H", 1))
            field_2.extend(struct.pack("B", 76))
            data.extend(field_2)

            return data

        except Exception as e:
            self.data_logger.exception(e)

    def __full_mode_msg(self) -> bytearray:

        """
        Create a message in bytearray for full mode connection.

        Returns:
            bytearray: The full mode message in bytearray format.
        """
        try:
            self.channels = [self.channel_num]
            data = bytearray()

            data.extend(struct.pack(">H", 0))

            data.extend(struct.pack("B", 12))

            data.extend(struct.pack("B", 2))

            channel_bits = 0
            for channel_num in self.channels:
                if channel_num < 64 and channel_num > 0:
                    channel_bits |= 1 << channel_num
            # Field-1
            field_1 = bytearray()
            field_1.extend(struct.pack("B", 1))
            field_1.extend(struct.pack(">H", 8))
            field_1.extend(struct.pack(">Q", channel_bits))
            data.extend(field_1)

            # Field-2
            field_2 = bytearray()
            field_2.extend(struct.pack("B", 2))
            field_2.extend(struct.pack(">H", 1))
            field_2.extend(struct.pack("B", 70))
            data.extend(field_2)

            return data

        except Exception as e:
            self.data_logger.exception(e)

    def __subscription_msg(self, symbols: list) -> bytearray:

        """
        Create a message in bytearray for symbol subscription.

        Args:
            symbols (list): A list of symbols to subscribe to.

        Returns:
            bytearray: The subscription message in bytearray format.
        """

        try:
            self.scrips_per_channel[self.channel_num] += symbols
            self.scrips = symbols
            self.scrips_data = bytearray()
            self.scrips_data.append(len(self.scrips) >> 8 & 0xFF)
            self.scrips_data.append(len(self.scrips) & 0xFF)
            for scrip in self.scrips:
                scrip_bytes = str(scrip).encode("ascii")
                self.scrips_data.append(len(scrip_bytes))
                self.scrips_data.extend(scrip_bytes)

            data_len = (
                18 + len(self.scrips_data) + len(self.__access_token) + len(self.source)
            )
            request_type = 4
            field_count = 2
            buffer_msg = bytearray()
            buffer_msg.extend(struct.pack(">H", data_len))
            buffer_msg.append(request_type)
            buffer_msg.append(field_count)

            # Field-1
            buffer_msg.append(1)
            buffer_msg.extend(struct.pack(">H", len(self.scrips_data)))
            buffer_msg.extend(self.scrips_data)

            # Field-2
            buffer_msg.append(2)
            buffer_msg.extend(struct.pack(">H", 1))
            buffer_msg.append(self.channel_num)
            return buffer_msg

        except Exception as e:
            self.data_logger.exception(e)

    def __unsubscription_msg(self, symbols: list) -> bytearray:
        """
        Create a message in bytearray for unsubscription message.

        Args:
            symbols (list): A list of symbols to unsubscribe from.

        Returns:
            bytearray: The unsubscription message in bytearray format.
        """
        try:
            scrips_data = bytearray()
            scrips_data.append(len(symbols) >> 8 & 0xFF)
            scrips_data.append(len(symbols) & 0xFF)
            for scrip in symbols:
                scrip_bytes = str(scrip).encode("ascii")
                scrips_data.append(len(scrip_bytes))
                scrips_data.extend(scrip_bytes)

            data_len = (
                18 + len(scrips_data) + len(self.__access_token) + len(self.source)
            )
            request_type = 5
            field_count = 2
            buffer_msg = bytearray()
            buffer_msg.extend(struct.pack(">H", data_len))
            buffer_msg.append(request_type)
            buffer_msg.append(field_count)

            # Field-1
            buffer_msg.append(1)
            buffer_msg.extend(struct.pack(">H", len(scrips_data)))
            buffer_msg.extend(scrips_data)

            # Field-2
            buffer_msg.append(2)
            buffer_msg.extend(struct.pack(">H", 1))
            buffer_msg.append(self.channel_num)

            return buffer_msg
        except Exception as e:
            self.data_logger.exception(e)

    def __channel_resume_msg(self, channel: int) -> bytearray:

        """
        Create a message in bytearray for channel resume.

        Args:
            channel (int): The channel to resume.

        Returns:
            bytearray: The channel resume message in bytearray format.
        """
        try:

            self.channels = [channel]

            data = bytearray()

            data.extend(struct.pack(">H", 0))

            data.extend(struct.pack("B", 8))

            data.extend(struct.pack("B", 1))

            channel_bits = 0
            for channel_num in self.channels:
                if channel_num < 64 and channel_num > 0:
                    channel_bits |= 1 << channel_num
            # Field-1
            field_1 = bytearray()
            field_1.extend(struct.pack("B", 1))
            field_1.extend(struct.pack(">H", 8))
            field_1.extend(struct.pack(">Q", channel_bits))
            data.extend(field_1)

            return data

        except Exception as e:
            self.data_logger.exception(e)

    def __channel_pause_msg(self, channel: int) -> bytearray:

        """
        Create a message in bytearray for channel pause.

        Args:
            channel (int): The channel to pause.

        Returns:
            bytearray: The channel pause message in bytearray format.
        """

        try:
            self.channels = [channel]

            data = bytearray()

            data.extend(struct.pack(">H", 0))

            data.extend(struct.pack("B", 7))

            data.extend(struct.pack("B", 1))

            channel_bits = 0
            for channel_num in self.channels:
                if channel_num < 64 and channel_num > 0:
                    channel_bits |= 1 << channel_num
            # Field-1
            field_1 = bytearray()
            field_1.extend(struct.pack("B", 1))
            field_1.extend(struct.pack(">H", 8))
            field_1.extend(struct.pack(">Q", channel_bits))
            data.extend(field_1)

            return data

        except Exception as e:
            self.data_logger.exception(e)

    def __ackowledgement_msg(self, message_number: int) -> bytearray:

        """
        Create a message in bytearray for acknowledgement.

        Args:
            message_number (int): The message number to acknowledge.

        Returns:
            bytearray: The acknowledgement message in bytearray format.
        """
        try:
            total_size = 11
            req_type = 3
            field_count = 1
            field_id = 1
            field_size = 4
            field_value = message_number
            buffer_msg = bytearray()
            # Pack the data into the byte array
            buffer_msg.extend(struct.pack(">H", total_size - 2))
            buffer_msg.extend(struct.pack("B", req_type))
            buffer_msg.extend(struct.pack("B", field_count))
            buffer_msg.extend(struct.pack("B", field_id))
            buffer_msg.extend(struct.pack(">H", field_size))
            buffer_msg.extend(struct.pack(">I", field_value))

            return buffer_msg

        except Exception as e:
            self.data_logger.exception(e)

    def __auth_resp(self, data: bytearray) -> dict:
        """
        Unpacks the authentication response from a bytearray.

        Args:
            data (bytearray): The authentication response message.

        Returns:
            dict: The authentication response as a dictionary with keys 'code', 'message', and 's'.
        """

        try:
            offset = 4
            offset += 1
            field_length = struct.unpack("!H", data[offset : offset + 2])[0]
            offset += 2
            string_val = data[offset : offset + field_length].decode("utf-8")
            offset += field_length

            if string_val == "K":

                self.On_message(
                    {   "type": defines.AUTH_TYPE,
                        "code": defines.SUCCESS_CODE,
                        "message": defines.AUTH_SUCCESS,
                        "s": defines.SUCCESS,
                    }
                )
            else:
                self.On_error(
                    {   "type": defines.AUTH_TYPE,
                        "code": defines.AUTH_ERROR_CODE,
                        "message": defines.AUTH_FAIL,
                        "s": defines.ERROR,
                    }
                )

            offset += 1
            field_length = struct.unpack("!H", data[offset : offset + 2])[0]
            offset += 2
            self.ack_count = struct.unpack(">I", data[offset : offset + 4])[0]
            offset += 4

        except Exception as e:
            self.data_logger.exception(e)

    def __subscribe_resp(self, data: bytearray) -> dict:
        """
        Unpacks the subscription response from a bytearray.

        Args:
            data (bytearray): The subscription response message.

        Returns:
            dict: The subscription response as a dictionary with keys 'code', 'message', and 's'.
        """

        try:

            offset = 5
            field_length = struct.unpack("H", data[offset : offset + 2])[0]
            offset += 2
            string_val = data[offset : offset + 1].decode("latin-1")
            offset += field_length
            if string_val == "K":

                self.On_message(
                    {   
                        "type": defines.SUBS_TYPE,
                        "code": defines.SUCCESS_CODE,
                        "message": defines.SUBSCRIBE_SUCCESS,
                        "s": defines.SUCCESS,
                    }
                )
            else:
                self.On_error(
                    {
                        "type": defines.SUBS_TYPE,
                        "code": defines.SUBS_ERROR_CODE,
                        "message": defines.SUBSCRIBE_FAIL,
                        "s": defines.ERROR,
                    }
                )

        except Exception as e:
            self.data_logger.exception(e)

    def __unsubscribe_resp(self, data: bytearray) -> dict:

        """
        Unpacks the unsubscription response from a bytearray.

        Args:
            data (bytearray): The unsubscription response message.

        Returns:
            dict: The unsubscription response as a dictionary with keys 'code', 'message', and 's'.
        """

        try:

            offset = 5
            field_length = struct.unpack("H", data[offset : offset + 2])[0]
            offset += 2
            string_val = data[offset : offset + 1].decode("latin-1")
            offset += field_length
            if string_val == "K":

                self.On_message(
                    {
                        "type": defines.UNSUBS_TYPE,
                        "code": defines.SUCCESS_CODE,
                        "message": defines.UNSUBSCRIBE_SUCCESS,
                        "s": defines.SUCCESS,
                    }
                )
                for symbol in self.unsub_symbol:
                    count = 0
                    for channel in self.running_channels:
                        if symbol in self.scrips_per_channel[channel]:
                            count +=1 
                        if count > 1:
                            break
                    if symbol in self.scrips_per_channel[self.active_channel]:
                        self.scrips_per_channel[self.active_channel].remove(symbol)
                    if count == 1:
                        self.symbol_token.pop(symbol)
                            
            else:
                self.On_error(
                    {
                        "type": defines.UNSUBS_TYPE,
                        "code": defines.UNSUBS_ERROR_CODE,
                        "message": defines.UNSUBSCRIBE_FAIL,
                        "s": defines.ERROR,
                    }
                )

        except Exception as e:
            self.data_logger.exception(e)

    def __lite_full_mode_resp(self, data: bytearray) -> dict:

        """
        Unpacks the lite/full mode response from a bytearray.

        Args:
            data (bytearray): The lite/full mode response message.

        Returns:
            dict: The lite/full mode response as a dictionary with keys 'code', 'message', and 's'.
        """

        try:
            offset = 3

            # Unpack the field count
            field_count = struct.unpack("!B", data[offset : offset + 1])[0]
            offset += 1

            if field_count >= 1:
                # Unpack the field ID
                offset += 1

                # Unpack the field length
                field_length = struct.unpack("!H", data[offset : offset + 2])[0]
                offset += 2

                # Extract the string value and decode it
                string_val = data[offset : offset + field_length].decode("utf-8")
                offset += field_length

                if string_val == "K":
                    if self.lite:
                        self.On_message(
                            {
                                "type": defines.LITE_MODE_TYPE,
                                "code": defines.SUCCESS_CODE,
                                "message": defines.LITE_MODE,
                                "s": defines.SUCCESS,
                            }
                        )
                    else:
                        self.On_message(
                            {
                                "type": defines.FULL_MODE_TYPE,
                                "code": defines.SUCCESS_CODE,
                                "message": defines.FULL_MODE,
                                "s": defines.SUCCESS,
                            }
                        )
                else:
                    self.On_error(
                        {
                            "code": defines.MODE_ERROR_CODE,
                            "message": defines.MODE_CHANGE_ERROR,
                            "s": defines.ERROR,
                        }
                    )

        except Exception as e:
            self.data_logger.exception(e)
        
    def __resume_pause_resp(self, data: bytearray, channeltype: int) -> dict:
        """
        Unpacks and processes the resume/pause response data based on the channel type.

        Args:
            data (bytearray): The response data.
            channeltype (int): The channel type. 7 for pause and 8 for resume.

        Returns:
            dict: The resume/pause response as a dictionary with keys 'code', 'message', and 's'.
        """
        try:
            offset = 5

            # Unpack the field length
            field_length = struct.unpack("!H", data[offset : offset + 2])[0]
            offset += 2

            # Extract the string value and decode it
            string_val = data[offset : offset + field_length].decode("utf-8")
            offset += field_length

            if string_val == "K":
                if channeltype == 7:
                    self.On_message(
                        {
                            "type": defines.CH_PAUSE_TYPE,
                            "code": defines.SUCCESS_CODE,
                            "message": defines.CHANNEL_PAUSED,
                            "s": defines.SUCCESS,
                        }
                    )
                else:
                    self.On_message(
                        {
                            "type": defines.CH_RESUME_TYPE,
                            "code": defines.SUCCESS_CODE,
                            "message": defines.CHANNEL_RESUMED,
                            "s": defines.SUCCESS,
                        }
                    )
            else:
                if channeltype == 7:
                    self.On_error(
                        {
                            "type": defines.CH_PAUSE_TYPE,
                            "code": defines.PAUSE_ERROR_CODE,
                            "message": defines.CHANNEL_CHANGE_FAIL,
                            "s": defines.ERROR,
                        }
                    )
                else:
                    self.On_error(
                        {
                            "type": defines.CH_RESUME_TYPE,
                            "code": defines.RESUME_ERROR_CODE,
                            "message": defines.CHANNEL_CHANGE_FAIL,
                            "s": defines.ERROR,
                        }
                    )

        except Exception as e:
            self.data_logger.exception(e)

    def __response_output(self, data: str, data_type: str) -> object:

        """
        Processes the response data and returns the output based on the specified data_type.

        Args:
            data (bytearray): The response data.
            data_type (str): The type of data to be processed.

        Returns:
            object: The processed output based on the specified data_type.
        """
        try:
            data_resp = data
            precision_calcu_value = [
                                    "ltp",
                                    "bid_price",
                                    "ask_price",
                                    "avg_trade_price",
                                    "low_price",
                                    "high_price",
                                    "open_price",
                                    "prev_close_price",
                                ]
            response = {}
            if (
                "bidPrice1" not in data_resp
                # and "vol_traded_today" in data_resp
                and self.lite
            ):
                for i, val in enumerate(self.lite_val):
                    if val in data_resp and val == "ltp":
                        response[val] = data_resp[val] / (
                                (10 ** data_resp["precision"] ) * data_resp["multiplier"]
                            )
                    else:
                        response[val] = data_resp[val]
                if "prev_close_price" in response and "ltp" in response:
                    response["ch"] = round((response['ltp']  - response['prev_close_price']),2) 
                    response["chp"] = round((response["ch"]  / response['prev_close_price'] * 100) , 2)
            else:
                if data_type == "depth":

                    for i, val in enumerate(self.depthvalue):
                        if val in data_resp and i < 10:
                            response[val] = data_resp[val] / ((
                                10 ** data_resp["precision"] ) * data_resp["multiplier"])

                        elif val in data_resp:
                            response[val] = data_resp[val]
                elif data_type == "scrips":
                    for i, val in enumerate(self.data_val):
                        if val in data_resp and val in precision_calcu_value and val not in ["upper_ckt", "lower_ckt"]:
                            response[val] = data_resp[val] / (
                                (10 ** data_resp["precision"] )* data_resp["multiplier"]
                            )
                            # response[val] = data_resp[val] / (

                        elif val in data_resp:
                            response[val] = data_resp[val]

                    response["lower_ckt"] = 0
                    response["upper_ckt"] = 0
                    if "prev_close_price" in response and "ltp" in response and response["prev_close_price"] != 0:
                        response["ch"] = round((response['ltp']  - response['prev_close_price']),4) 
                        response["chp"] = round((response["ch"]  / response['prev_close_price'] * 100) , 4)
                    if "OI" in response:
                        response.pop("OI")
                    if "Yhigh" in response:
                        response.pop("Yhigh")
                    if "Ylow" in response:
                        response.pop("Ylow")
                else:
                    for i, val in enumerate(self.index_val):
                        if val in data_resp and i in [0, 1, 3, 4, 5]:
                            response[val] = data_resp[val] / (
                                (10 ** data_resp["precision"] ) * data_resp["multiplier"]
                            )
                        elif val in data_resp:
                            response[val] = data_resp[val]
                        if "prev_close_price" in response and "ltp" in response:
                            response["ch"] = round((response['ltp']  - response['prev_close_price']),2) 
                            response["chp"] = round((response["ch"]  / response['prev_close_price'] * 100) , 2)
            
            self.On_message(response)

        except Exception as e:
            self.data_logger.exception(e)

    def __datafeed_resp(self, data: bytearray):

        """
        Unpacks and processes the data based on data_type and sends it to the __response_output function.

        Args:
            data (bytearray): The response data.

        Returns:
            None
        """
        try:

            if self.ack_count > 0:
                self.update_count += 1
                message_num = struct.unpack(">I", data[3:7])[0]
                if self.update_count == self.ack_count:
                    self.ack_msg = self.__ackowledgement_msg(message_num)
                    # self.message.append(self.ack_msg)
                    self.add_message(self.ack_msg)

                    
                    self.update_count = 0
            scrip_count = struct.unpack("!H", data[7:9])[0]
            offset = 9

            for _ in range(scrip_count):
                data_type = struct.unpack("B", data[offset : offset + 1])[0]
                if data_type == 83:  # Snapshot datafeed

                    offset += 1
                    topic_id = struct.unpack("H", data[offset : offset + 2])[0]
                    offset += 2
                    topic_name_len = struct.unpack("B", data[offset : offset + 1])[0]
                    offset += 1

                    topic_name = data[offset : offset + topic_name_len].decode("utf-8")
                    offset += topic_name_len

                    # Maintaining dict - topic_id : topic_name
                    if topic_name[:2] == "dp":
                        self.dp_sym[topic_id] = topic_name

                        self.resp[self.dp_sym[topic_id]] = {}

                        field_count = struct.unpack("B", data[offset : offset + 1])[0]
                        offset += 1

                        for index in range(field_count):
                            value = struct.unpack(">i", data[offset : offset + 4])[0]
                            offset += 4

                            if value != -2147483648:
                                self.resp[self.dp_sym[topic_id]][
                                    self.depthvalue[index]
                                ] = value

                        offset += 2

                        multiplier = struct.unpack(">H", data[offset : offset + 2])[0]
                        self.resp[self.dp_sym[topic_id]]["multiplier"] = multiplier
                        offset += 2
                        precision = struct.unpack("B", data[offset : offset + 1])[0]
                        self.resp[self.dp_sym[topic_id]]["precision"] = precision
                        offset += 1

                        val = ["exchange", "exchange_token", "symbol"]
                        for i in range(3):
                            string_len = struct.unpack("B", data[offset : offset + 1])[
                                0
                            ]
                            offset += 1
                            string_data = data[offset : offset + string_len].decode(
                                "utf-8",errors='ignore'
                            )
                            self.resp[self.dp_sym[topic_id]][val[i]] = string_data
                            offset += string_len
                        self.resp[self.dp_sym[topic_id]]["type"] = "dp"
                        self.resp[topic_name]["symbol"] = self.symbol_token[topic_name]
                        self.__response_output(
                            self.resp[self.dp_sym[topic_id]], "depth"
                        )

                    elif topic_name[:2] == "if":

                        self.index_sym[topic_id] = topic_name
                        self.resp[self.index_sym[topic_id]] = {}

                        # field_count - 21 in scrips , 25 in depth , 6 in index
                        field_count = struct.unpack("B", data[offset : offset + 1])[0]
                        offset += 1

                        for index in range(field_count):

                            value = struct.unpack(">i", data[offset : offset + 4])[0]
                            offset += 4

                            if value != -2147483648:
                                self.resp[self.index_sym[topic_id]][
                                    self.index_val[index]
                                ] = value

                        offset += 2

                        multiplier = struct.unpack(">H", data[offset : offset + 2])[0]
                        self.resp[self.index_sym[topic_id]]["multiplier"] = multiplier
                        offset += 2

                        precision = struct.unpack("B", data[offset : offset + 1])[0]
                        self.resp[self.index_sym[topic_id]]["precision"] = precision
                        offset += 1

                        val = ["exchange", "exchange_token", "symbol"]
                        for i in range(3):
                            string_len = struct.unpack("B", data[offset : offset + 1])[
                                0
                            ]
                            offset += 1
                            string_data = data[offset : offset + string_len].decode(
                                "utf-8",errors='ignore'
                            )
                            self.resp[self.index_sym[topic_id]][val[i]] = string_data
                            offset += string_len
                        self.resp[topic_name]["symbol"] = self.symbol_token[topic_name]
                        self.resp[self.index_sym[topic_id]]["type"] = "if"
                        self.__response_output(
                            self.resp[self.index_sym[topic_id]], "index"
                        )

                    elif topic_name[:2] == "sf":
                        self.scrips_sym[topic_id] = topic_name
                        self.resp[self.scrips_sym[topic_id]] = {}

                        # field_count - 21 in scrips , 25 in depth , 6 in index
                        field_count = struct.unpack("B", data[offset : offset + 1])[0]
                        offset += 1

                        for index in range(field_count):
                            value = struct.unpack(">i", data[offset : offset + 4])[0]
                            offset += 4
                            if value != -2147483648:
                                self.resp[self.scrips_sym[topic_id]][
                                    self.data_val[index]
                                ] = value

                        offset += 2

                        multiplier = struct.unpack(">H", data[offset : offset + 2])[0]
                        self.resp[self.scrips_sym[topic_id]]["multiplier"] = multiplier
                        offset += 2

                        precision = struct.unpack("B", data[offset : offset + 1])[0]
                        self.resp[self.scrips_sym[topic_id]]["precision"] = precision
                        offset += 1
                        val = ["exchange", "exchange_token", "symbol"]
                        for i in range(3):
                            string_len = struct.unpack("B", data[offset : offset + 1])[
                                0
                            ]
                            offset += 1
                            string_data = bytes(data[offset : offset + string_len]).decode(
                                "utf-8" ,errors='ignore'
                            )
                            self.resp[self.scrips_sym[topic_id]][val[i]] = string_data
                            offset += string_len
                        self.resp[topic_name]["symbol"] = self.symbol_token[topic_name]
                        self.resp[self.scrips_sym[topic_id]]["type"] = "sf"
                        self.__response_output(
                            self.resp[self.scrips_sym[topic_id]], "scrips"
                        )

                elif data_type == 85:  # Full mode datafeed
                    offset += 1
                    topic_id = struct.unpack("H", data[offset : offset + 2])[0]
                    offset += 2

                    field_count = struct.unpack("B", data[offset : offset + 1])[0]
                    offset += 1
                    sf_flag, idx_flag, dp_flag = False, False, False
                    self.UpdateTick = False
                    for index in range(field_count):
                        value = struct.unpack(">i", data[offset : offset + 4])[0]
                        offset += 4
                        # if field_count == 20 or field_count == 21:
                        if topic_id in self.scrips_sym:
                            if self.data_val[index] in self.resp[self.scrips_sym[topic_id]] and self.resp[self.scrips_sym[topic_id]][
                                self.data_val[index]] != value and value != -2147483648:
                                self.resp[self.scrips_sym[topic_id]][
                                    self.data_val[index]
                                ] = value
                                self.UpdateTick = True
                            elif self.data_val[index] not in self.resp[self.scrips_sym[topic_id]] and value != -2147483648:
                                self.resp[self.scrips_sym[topic_id]][
                                    self.data_val[index]
                                ] = value
                                self.UpdateTick = True

                            sf_flag = True
                        elif topic_id in self.index_sym:
                            if self.index_val[index] in self.resp[self.index_sym[topic_id]] and  self.resp[self.index_sym[topic_id]][self.index_val[index]] != value and value != "-2147483648":

                                self.resp[self.index_sym[topic_id]][
                                    self.index_val[index]
                                ] = value
                                self.UpdateTick = True
                            elif self.index_val[index] not in self.resp[self.index_sym[topic_id]] and value != -2147483648:
                                self.resp[self.index_sym[topic_id]][
                                    self.index_val[index]
                                ] = value
                                self.UpdateTick = True
                            idx_flag = True
                        elif topic_id in self.dp_sym:
                            if self.depthvalue[index] in self.resp[self.dp_sym[topic_id]] and self.resp[self.dp_sym[topic_id]][
                                self.depthvalue[index]] != value and value != -2147483648:
                                self.resp[self.dp_sym[topic_id]][
                                    self.depthvalue[index]
                                ] = value
                                self.UpdateTick = True
                            elif self.depthvalue[index] not in self.resp[self.dp_sym[topic_id]] and  value != -2147483648:
                                self.resp[self.dp_sym[topic_id]][
                                    self.depthvalue[index]
                                ] = value
                                self.UpdateTick = True                                
                            dp_flag = True
                    if self.UpdateTick:
                        if sf_flag:
                            self.__response_output(
                                self.resp[self.scrips_sym[topic_id]], "scrips"
                            )
                        elif idx_flag:
                            self.__response_output(
                                self.resp[self.index_sym[topic_id]], "index"
                            )
                        elif dp_flag:
                            self.__response_output(
                                self.resp[self.dp_sym[topic_id]], "depth"
                            )

                elif data_type == 76:  # lite mode datafeed

                    offset += 1
                    topic_id = struct.unpack("H", data[offset : offset + 2])[0]
                    offset += 2
                    sf_flag, idx_flag = False, False
                    if topic_id in self.scrips_sym:

                        # for index in range(3):
                        value = struct.unpack(">i", data[offset : offset + 4])[0]
                        offset += 4
                        if value != self.resp[self.scrips_sym[topic_id]][self.data_val[0]] and value != -2147483648:
                            self.resp[self.scrips_sym[topic_id]][self.data_val[0]] = value
                            sf_flag = True
                            self.resp[self.scrips_sym[topic_id]]["type"] = "sf"
                            self.__response_output(
                                self.resp[self.scrips_sym[topic_id]], "scrips"
                            )
                    elif topic_id in self.index_sym:
                        value = struct.unpack(">i", data[offset : offset + 4])[0]
                        offset += 4
                        if value != self.resp[self.index_sym[topic_id]][self.index_val[0]] and value != -2147483648:
                            self.resp[self.index_sym[topic_id]][self.index_val[0]] = value
                            idx_flag = True
                            self.resp[self.index_sym[topic_id]]["type"] = "if"
                        
                            self.__response_output(
                                self.resp[self.index_sym[topic_id]], "index"
                            )
                        
        except Exception as e:
            self.data_logger.exception(e)


    def __response_msg(self, data: bytearray):
        """
        Processes the response message based on the response type and calls the corresponding function.

        Args:
            data (bytearray): The response data.

        """
        try:

            _, resp_type = struct.unpack("!HB", data[:3])
            if resp_type == 1:  # Authentication response
                self.__auth_resp(data)

            elif resp_type == 4:  # subsciption response
                self.__subscribe_resp(data)

            elif resp_type == 5:  # Unsubsciption response
                self.__unsubscribe_resp(data)

            elif resp_type == 6:  # Data Feed Response
                self.__datafeed_resp(data)

            elif resp_type == 7 or resp_type == 8:
                self.__resume_pause_resp(data, resp_type)

            elif resp_type == 12:  # Full Mode Data Response
                self.__lite_full_mode_resp(data)

        except Exception as e:
            self.data_logger.exception(e)

    def __symbol_conversion(self, symbolslst: list) -> dict:
        """
        Converts symbols to HSM symbol tokens and returns a dictionary of {hsmtoken: symbol}.

        Args:
            symbolslst (list): A list of symbols to convert.

        Returns:
            dict: A dictionary mapping HSM symbol tokens to symbols.
        """
        try:

            wrong_symbols = []
            symb_flag = False
            idx_dp_flag = False
            symbol_dict = {}
            total_symbols = len(symbolslst)
            if (
                len(self.scrips_per_channel[self.channel_num]) > self.symbol_limit
                or total_symbols > self.symbol_limit
                and len(self.scrips_per_channel[self.channel_num]) + total_symbols > self.symbol_limit
            ):
                self.On_error(
                    {
                        "code": defines.LIMIT_EXCEED_CODE,
                        "message": defines.LIMIT_EXCEED_MSG_5000,
                        "s": defines.ERROR,
                    }
                )
                return 

            symbol_chunks = [
                symbolslst[i : i + 500] for i in range(0, total_symbols, 500)
            ]
            conv = SymbolConversion(self.__access_token, self.data_type, self.log_path)
            for symbols in symbol_chunks:
                symbol_value = conv.symbol_to_hsmtoken(symbols)
                if symbol_value[3] != "":
                    self.On_error(
                    {
                        "code": defines.INVALID_CODE,
                        "message": symbol_value[3],
                        "s": defines.ERROR,
                        "type": defines.SUBS_TYPE
                    })
                    return
                symbol_dict.update(symbol_dict, **symbol_value[0])
                if type(symbol_value[1]) == list and len(symbol_value[1]) > 0:

                    wrong_symbols += symbol_value[1]
                    symb_flag = True
                if symbol_value[2] == True:
                    idx_dp_flag = True
            
                
            if symb_flag:
                self.On_error(
                    {
                        "code": defines.INVALID_CODE,
                        "message": defines.INVALID_SYMBOLS,
                        "s": defines.ERROR,
                        "type": defines.SUBS_TYPE,
                        "invalid_symbols": wrong_symbols,
                    }
                )
            if idx_dp_flag:
                self.On_error(
                    {
                        "code": defines.INVALID_CODE,
                        "message": defines.INDEX_DEPTH_ERROR_MESSAGE,
                        "s": defines.ERROR,
                        "type": defines.SUBS_TYPE,
                    }
                )

            return symbol_dict

        except Exception as e:
            self.data_logger.exception(e)

    def __channel_resume_pause(self):
        """
        Pauses the active channel and resumes the specified channel if necessary.

        If the WebSocket object (__ws_object) is not None and there is an active channel (active_channel)
        that is different from the specified channel (channelNum), the function creates and appends a pause message
        for the active channel to the message list. If the specified channel is already in the running_channels set,
        the function creates and appends a resume message for the specified channel to the message list.

        Finally, it updates the running_channels set and sets the active_channel to the specified channel (channelNum).

        """
        try:
            if (
                self.__ws_object is not None
                and self.active_channel is not None
                and self.active_channel != self.channel_num
            ):
                message = self.__channel_pause_msg(self.active_channel)
                # self.message.append(message)
                self.add_message(message)


                if self.channel_num in self.running_channels:
                    message = self.__channel_resume_msg(self.channel_num)
                    # self.message.append(message)
                    self.add_message(message)

            self.running_channels.add(self.channel_num)
            self.active_channel = self.channel_num

        except Exception as e:
            self.data_logger.exception(e)

    def channel_resume(self, channel: int) -> None:
        """
        Resumes the specified channel.

        Args:
            channel (int): The channel number to resume.
        """
        try:
            self.channel_num = channel
            self.__channel_resume_pause()

        except Exception as e:
            self.data_logger.exception(e)

    def On_message(self, message: dict) -> None:
        """
        Handles the received message.

        Args:
            message (str): The received message.
        """
        try:
            if self.OnMessage is not None:
                self.OnMessage(message)
            else:
                if self.write_to_file:
                    self.data_logger.debug(f"Response:{message}")
                else:
                    print(f"Response:{message}")

        except KeyError as e:
            key_name = str(e)
            self.data_logger.exception(e)
            self.On_error(f"KeyError: The key {key_name} is missing in the response.")

        except Exception as e:
            self.data_logger.exception(e)
            self.On_error(e)

    def __send_message(self, message: str) -> None:
        """
        Sends a message through the WebSocket connection.

        Args:
            message (str): The message to send.
        """
        with self.websocket_lock:
            if self.__ws_object is not None:

                self.__ws_object.send(message, opcode=websocket.ABNF.OPCODE_BINARY)


    def add_message(self, message):
        """
        Add a message to the list of messages and notify waiting threads.

        Args:
            message (str): The message to add to the list.
        """
        with self.message_lock:
            self.message.append(message)
            self.message_condition.notify()


    def __process_message_queue(self) -> None:
        """
        Processes the message queue by sending messages sequentially.
        """

        while not self.message_thread_stop_event.is_set():
            with self.message_lock:
                while not self.message_thread_stop_event.is_set() and  not self.message:  # Use a loop to handle spurious wake-ups
                    self.message_condition.wait()
                if self.message_thread_stop_event.is_set():
                    break
                # Once a message is available, pop it from the queue
                message = self.message.pop(0)

            # Send the message outside the lock to avoid blocking other threads
            self.__send_message(message)
            # time.sleep(0.01)


    def On_error(self, message: dict) -> None:
        """
        Handles the error message.

        Args:
            message (str): The error message.
        """
        if self.OnError is not None:
            self.OnError(message)
            self.data_logger.error(message)
        else:
            if self.write_to_file:
                self.data_logger.debug(f"ERROR Response:{message}")
            else:
                print(f"Error: {message}")




    def on_open(self) -> None:
        """
        Handles the open action.
        """
        try:
            if self.OnOpen:
                self.OnOpen()
        except Exception as e:
            self.data_logger.exception(e)
            self.On_error(e)


    def connect(self) -> None:
        """
        Establishes a connection to the WebSocket.

        If the WebSocket object is not already initialized, this method will create the
        WebSocket connection.

        """
        try:
            if self.__ws_object is None:
                self.__init_connection()
                time.sleep(2)
            self.on_open()

        except Exception as e:
            self.data_logger.exception(e)
            self.On_error(e)
            

    def on_close(self, message: dict) -> None:
        """
        Handles the close event.

        Args:
            message (dict): The close message .
        """
        try:
            if self.OnClose:
                self.OnClose(message)
            else:
                print(f"Response: {message}")
        except Exception as e:
            self.data_logger.exception(e)
            self.On_error(e)


    def __on_open(self, ws) -> None:
        """
        Handles the WebSocket connection open event.

        Args:
            ws (websocket.WebSocketApp): The WebSocket object.
        """
        if self.__ws_object is None:
            self.message = []
            self.__ws_object = ws
            self.message_thread = Thread(target=self.__process_message_queue)
            self.ping_thread = Thread(target=self.__ping)
            self.message_thread_stop_event = threading.Event()  # Event to signal stopping
            self.message_thread.start()
            self.ping_thread.start()
            message = self.__access_token_msg()
            # self.message.append(message)
            self.add_message(message)
            self.reconnect_attempts = 0
            self.reconnect_delay = 0
            if self.lite:
                message = self.__lite_mode_msg()
                # self.message.append(message)
                self.add_message(message)
            else:
                message = self.__full_mode_msg()
                # self.message.append(message)
                self.add_message(message)


    def __on_close(self, ws, close_code, close_reason):
        """
        Handle the WebSocket connection close event.

        Args:
            ws (WebSocket): The WebSocket object.
            close_code (int): The code indicating the reason for closure.
            close_reason (str): The reason for closure.

        Returns:
            dict: A dictionary containing the response code, message, and s.
        """
        if self.restart_flag:
            if self.reconnect_attempts < self.max_reconnect_attempts:

                if self.write_to_file:
                    self.data_logger.debug(
                        f"Response:{f'Attempting reconnect {self.reconnect_attempts+1} of {self.max_reconnect_attempts}...'}"
                    )
                else:
                    print(
                        f"Attempting reconnect {self.reconnect_attempts+1} of {self.max_reconnect_attempts}..."
                    )

                if (self.reconnect_attempts) % 5 == 0:
                    self.reconnect_delay += 5
                time.sleep(self.reconnect_delay)
                self.reconnect_attempts += 1

                self.__ws_object = None
                self.scrips_per_channel[self.channel_num] = []
                self.symbol_token = {}

                self.connect()
            else:
                if self.write_to_file:
                    self.data_logger.debug(
                        f"Response:{'Max reconnect attempts reached. Connection abandoned.'}"
                    )
                else:
                    print("Max reconnect attempts reached. Connection abandoned.")
        else:

            self.on_close(
                {
                    "code": defines.SUCCESS_CODE,
                    "message": defines.CONNECTION_CLOSED,
                    "s": defines.SUCCESS,
                }
            )

    def __ping(self):
        while (
            self.__ws_object is not None
            and self.__ws_object.sock
            and self.__ws_object.sock.connected
        ):
            self.__ws_object.send(bytes([0, 1, 11]), opcode=websocket.ABNF.OPCODE_BINARY)
            time.sleep(10)

    def __init_connection(self):
        """
        Initializes the WebSocket connection and starts the WebSocketApp.

        The method creates a WebSocketApp object with the specified URL and sets the appropriate event handlers.
        It then starts the WebSocketApp in a separate thread.
        """
        try:
            if  self.access_token_to_hsmtoken() and self.__valid_token :
                if self.write_to_file:
                    self.background_flag = True  
                ws = websocket.WebSocketApp(
                    self.__url,
                    on_message=lambda ws, msg: self.__response_msg(msg),
                    on_error=lambda ws, msg: self.On_error(msg),
                    on_close=lambda ws, close_code, close_reason: self.__on_close(
                        ws, close_code, close_reason
                    ),
                    on_open=lambda ws: self.__on_open(ws),
                )

                self.ws_thread = Thread(target=ws.run_forever)
                self.ws_thread.daemon = self.background_flag
                self.ws_thread.start()

        except Exception as e:
            self.data_logger.exception(e)

    def close_connection(self) -> None:
        """
        Closes the WebSocket connection 

        """

        if self.__ws_object:
            self.restart_flag = False
            self.__ws_object.close()
            self.__ws_object = None
            self.ws_thread.join()
            self.message_thread_stop_event.set()
            with self.message_lock:
                self.message_condition.notify()  # Notify the thread to wake up
            self.message_thread.join()
            self.ping_thread.join()
            self.__ws_run = False
            self.scrips_per_channel[self.channel_num] = []


    def keep_running(self):
        """
        Starts an infinite loop to keep the program running.

        """
        self.__ws_run = True
        self.infy_loop = Thread(target=self.infinite_loop)
        self.infy_loop.start()

    def infinite_loop(self):
        while self.__ws_run:
            time.sleep(0.5)


    def is_connected(self):
        """
        Check if the websocket is connected.

        Returns:
            bool: True if the websocket is connected, False otherwise.
        """
        if self.__ws_object:
            return True
        else:
            return False

    def unsubscribe(
        self, symbols: list, data_type: str = "SymbolUpdate", channel: int = 11
    ):
        """
        Unsubscribes from real-time data updates for the specified symbols.

        Args:
            symbols (list): A list of symbols to unsubscribe from.
            data_type (str, optional): The type of data to unsubscribe from. Defaults to "SymbolUpdate".
            channel (int, optional): The channel to use for unsubscription. Defaults to 1.
        """
        try:
            if self.__valid_token:
                self.data_type = data_type
                self.symbols = symbols
                self.channel_num = channel
                self.__channel_resume_pause()
                self.channel_symbol = self.__symbol_conversion(symbols)
                self.unsub_symbol = list(self.channel_symbol.keys())
                for symb in self.unsub_symbol:
                    if symb not in self.scrips_count[self.channel_num]:
                        self.unsub_symbol.remove(symb)

                if len(self.unsub_symbol) != 0:
                    total_symbols = len(self.unsub_symbol)
                    symbol_chunks = [
                        self.unsub_symbol[i : i + 1500] for i in range(0, total_symbols, 1500)
                    ]
                    for symbols in symbol_chunks:
                        message = self.__unsubscription_msg(symbols)
                        # self.message.append(message)
                        self.add_message(message)

                else:
                    self.On_error(
                        {
                            "code": defines.INVALID_CODE,
                            "message": defines.INVALID_SYMBOLS,
                            "s": defines.ERROR,
                        }
                    )

        except Exception as e:
            self.data_logger.exception(e)

    def subscribe(
        self, symbols: list, data_type: str = "SymbolUpdate", channel: int = 11
    ):
        """
        Subscribes to real-time data updates for the specified symbols.

        Args:
            symbols (list): A list of symbols to subscribe to.
            data_type (str, optional): The type of data to subscribe to. Defaults to "SymbolUpdate".
            channel (int, optional): The channel to use for subscription. Defaults to 1.
        """
        try:
            if self.__valid_token:
                self.data_type = data_type
                self.symbols = symbols
                self.channel_num = channel
                self.__channel_resume_pause()
                self.channel_symbol = self.__symbol_conversion(symbols)
                if self.channel_symbol is None:
                    return
                if len(self.symbol_token) + len(self.channel_symbol) > 5000:
                    self.On_error(
                        {
                            "code": defines.LIMIT_EXCEED_CODE,
                            "message": defines.LIMIT_EXCEED_MSG_5000,
                            "s": defines.ERROR,
                        }
                    )
                    return
                self.symbol_token.update(self.symbol_token, **self.channel_symbol)
                self.scrips_count[self.channel_num] = list(self.channel_symbol.keys())
                total_symbols = len(self.scrips_count[self.channel_num])
                symbol_chunks = [
                    self.scrips_count[self.channel_num][i : i + 1500]
                    for i in range(0, total_symbols, 1500)
                ]
                for symbols in symbol_chunks:
                    message = self.__subscription_msg(symbols)
                    # self.message.append(message)
                    time.sleep(0.5)
                    self.add_message(message)


        except Exception as e:
            self.data_logger.exception(e)

import json
import threading
import websocket
import logging
from time import sleep

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def reportmsg(msg):
    # print(msg)
    logger.debug(msg)

class FeedType:
    TOUCHLINE = 1
    SNAPQUOTE = 2

class NorenApi:
    __service_config = {
        'websocket_endpoint': 'wss://api.shoonya.com/NorenWSTP/',
    }

    def __init__(self,
                 message_update_callback = None,
                 socket_open_callback = None,
                 socket_close_callback = None,
                 socket_error_callback = None,
                 susertoken = None,
                 userid = None,
                 retry_attempts = 5,
                 ):
        self.__websocket = None
        self.__websocket_connected = False
        self.__ws_mutex = threading.Lock()
        self.__on_error = None
        self.__on_disconnect = None
        self.__on_open = None
        self.__message_callback = None
        self.__order_update_callback = None
        self.__subscribers = {}
        self.__market_status_messages = []
        self.__exchange_messages = []
        self.max_retry_attempts = 5
        self.current_retry_attempt = 0
        self.retry_delay_seconds = 5
        self.__username   = userid
        self.__accountid  = userid
        self.__susertoken = susertoken
        self.max_retry_attempts = retry_attempts if retry_attempts > -1 else self.max_retry_attempts
        self.__on_open = socket_open_callback
        self.__on_disconnect = socket_close_callback
        self.__on_error = socket_error_callback
        self.__message_callback = message_update_callback

    def __ws_send(self, *args, **kwargs):
        while self.__websocket_connected == False:
            sleep(0.05)  # sleep for 50ms if websocket is not connected, wait for reconnection
        with self.__ws_mutex:
            ret = self.__websocket.send(*args, **kwargs)
        return ret

    def __on_close_callback(self, wsapp, close_status_code, close_msg):
        reportmsg(close_status_code)
        reportmsg(wsapp)

        self.__websocket_connected = False
        if self.__on_disconnect:
            self.__on_disconnect()

    def __on_open_callback(self, ws=None):
        self.__websocket_connected = True
        #prepare the data
        values              = { "t": "c" }
        values["uid"]       = self.__username
        values["actid"]     = self.__username
        values["susertoken"]    = self.__susertoken
        values["source"]    = 'API'

        payload = json.dumps(values)
        reportmsg(payload)
        self.__ws_send(payload)
        self.current_retry_attempt = 0
        #self.__resubscribe()


    def __on_error_callback(self, ws=None, error=None):
        if(type(ws) is not websocket.WebSocketApp): # This workaround is to solve the websocket_client's compatiblity issue of older versions. ie.0.40.0 which is used in upstox. Now this will work in both 0.40.0 & newer version of websocket_client
            error = ws
        if self.__on_error:
            self.__on_error(error)
        self.retry_connect()

    def __on_data_callback(self, ws=None, message=None, data_type=None, continue_flag=None):
        res = json.loads(message)
        # print(res)
        if(self.__message_callback is not None):
            if res['t'] == 'tk' or res['t'] == 'tf':
                self.__message_callback(res)
                return
            if res['t'] == 'dk' or res['t'] == 'df':
                self.__message_callback(res)
                return

        if(self.__on_error is not None):
            if res['t'] == 'ck' and res['s'] != 'OK':
                self.__on_error(res)
                return

        if self.__on_open:
            if res['t'] == 'ck' and res['s'] == 'OK':
                self.__on_open()
                return

    def ws_run_forever(self):
        self.__websocket.run_forever( ping_interval=3,  ping_payload='{"t":"h"}')#, ping_timeout=9)

    def start_websocket(self):
        """ Start a websocket connection for getting live data """
        url = self.__service_config['websocket_endpoint'].format(access_token=self.__susertoken)
        reportmsg('connecting to {}'.format(url))
        try:
            self.__websocket = websocket.WebSocketApp(url,
                                                      on_data=self.__on_data_callback,
                                                      on_error=self.__on_error_callback,
                                                      on_close=self.__on_close_callback,
                                                      on_open=self.__on_open_callback,
                                                      )
            # self.__websocket.run_forever( ping_interval=2,  ping_payload='{"t":"h"}', ping_timeout=1)
            self.__ws_thread = threading.Thread(target=self.ws_run_forever)
            self.__ws_thread.daemon = False
            self.__ws_thread.start()

        except Exception as e:
            logger.error(f"Error starting websocket: {e}")
            self.retry_connect()

    def close_websocket(self):
        if self.__websocket_connected == False:
            return
        self.__websocket_connected = False
        self.__websocket.close()
        self.__ws_thread.join()
        self.__ws_thread = None

    def subscribe(self, instrument, feed_type=FeedType.TOUCHLINE):
        values = {}

        if(feed_type == FeedType.TOUCHLINE):
            values['t'] =  't'
        elif(feed_type == FeedType.SNAPQUOTE):
            values['t'] =  'd'
        else:
            values['t'] =  str(feed_type)

        if type(instrument) == list:
            values['k'] = '#'.join(instrument)
        else :
            values['k'] = instrument

        data = json.dumps(values)

        #print(data)
        self.__ws_send(data)

    def unsubscribe(self, instrument, feed_type=FeedType.TOUCHLINE):
        values = {}

        if(feed_type == FeedType.TOUCHLINE):
            values['t'] =  'u'
        elif(feed_type == FeedType.SNAPQUOTE):
            values['t'] =  'ud'

        if type(instrument) == list:
            values['k'] = '#'.join(instrument)
        else :
            values['k'] = instrument

        data = json.dumps(values)

        #print(data)
        self.__ws_send(data)

    def retry_connect(self):
        if self.current_retry_attempt < self.max_retry_attempts:
            self.current_retry_attempt += 1
            sleep(self.retry_delay_seconds)
            print(f"Retrying connection Attempt {self.current_retry_attempt} of {self.max_retry_attempts}")
            self.start_websocket()
        else:
            logger.error("Max retry attempts reached. Exiting")
            self.close_websocket()

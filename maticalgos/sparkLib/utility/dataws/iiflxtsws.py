import datetime
import threading
import json
import traceback
import queue
import time
import os 
from .dependencies import XTSMDSocket_io, XTS_MarketData, SparkLib

class IIFLXTSWS():
    tmdiff = (datetime.datetime(1980, 1, 1, 0, 0, 0) - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
    apiURL = 'https://ttblaze.iifl.com/apimarketdata' 
    URL = "https://ttblaze.iifl.com"
    TOKENS = {}
    BROKER = "IIFLXTS"

    def __init__(self, accountData, accessToken = None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) :
        self.accountData = accountData
        self.dType = 1501 if dataType == "ltp" else 1502 if dataType == "depth" else None #Currently Only supports LTP Updates.
        self.onLtp = self.__onLTP if not onLtp  else onLtp
        self.onDepth = self.__onDepth if not onDepth else onDepth
        self.onError = self.__onError if not onError else onError
        self.onClose = self.__onClose if not onClose else onClose
        self.onOpen = self.__onOpen if not onOpen else onOpen
        self.actionQueue = queue.Queue()
        self.run = True
        if not accessToken :
            if os.environ.get("MATICALGOS_AccessToken"):
                accessToken = os.environ["MATICALGOS_AccessToken"] 
            else: 
                raise Exception("Please generate access token.")
        self.spk = SparkLib(access_token=accessToken)
        
    def getTokens(self, tokens):
        data = self.spk.getBrokerTokens(tokens=tokens, broker=self.BROKER)
        if data['status'] and not data['error'] : 
            tokens = data['data']
            return tokens
        else: 
            raise Exception (data['message'])
    
    def getSymbol(self, exchange, token):
        return self.TOKENS[", ".join([str(exchange), str(token)])]
    
    def onLTPHandler(self, message):
        try: 
            data = json.loads(message)
            touchlineData = data if not data.get("Touchline") else data['Touchline']
            timestamp = self.tmdiff + data['ExchangeTimeStamp'] if data.get("ExchangeTimeStamp") else self.tmdiff + data['LastUpdateTime']  #touchlineData['LastUpdateTime']
            dt = datetime.datetime.utcfromtimestamp(timestamp)
            token = self.getSymbol(data['ExchangeSegment'], data['ExchangeInstrumentID'])
            msg = {"timestamp_str" : str(dt), 
                   "timestamp" : str(int(float(timestamp))),
                   "symbol" : token, 
                   "ltp" : float(touchlineData['LastTradedPrice']),
                   "prev_day_close" : float(touchlineData['Close']), 
                   "oi" : 0,
                   "prev_day_oi" : 0,
                   "turnover" : int(touchlineData['TotalTradedQuantity']), 
                   "best_bid_price" : float(touchlineData['BidInfo']['Price']), 
                   "best_ask_price" : float(touchlineData['AskInfo']['Price']), 
                   "best_bid_qty" : int(touchlineData['BidInfo']['Size']),
                   "best_ask_qty" : int(touchlineData['AskInfo']['Size']), 
                   "ttq" : int(touchlineData['TotalTradedQuantity']),  
                   "token" : token}
            self.onLtp(msg)
                
        except Exception as e : 
            self.onError(f"Error : {e}, Traceback : {traceback.format_exc()}, data : {message}")

    def onDepthHandler(self, message):
        self.onDepth(message)

    def __onLTP(self, message):
        print(message)
    
    def __onDepth(self, message):
        print(message)
    
    def __onError(self, message):
        print("error : ",message)

    def __onClose(self):
        print("Connection Closed")

    def __onOpen(self):
        print("Connection Opened")

    def Subscribe(self, tokens):
        try: 
            notFound = []
            tokens = self.getTokens(tokens)
            Instruments = []
            for tk in tokens:
                wsToken = tk['wsToken']
                if wsToken != None : 
                    exch, tok = wsToken.split(", ")
                    Instruments.append({"exchangeSegment" : exch, 'exchangeInstrumentID' : int(tok)})
                    self.TOKENS[wsToken] = tk['token']
                else: 
                    notFound.append(tk['token'])
            
            if Instruments != [] : 
                response = self.xtconn.subscribe(Instruments, self.dType)
                if response.get('error') :
                    self.onError(f"Error in token Subscription : {response}")
            if notFound != [] : 
                self.onError(f"ERROR : Unable to find tokens for {notFound} ")
        except Exception as e : 
            self.onError(f"Error in token Subscription : {e}, Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        try: 
            notFound = []
            tokens = self.getTokens(tokens)
            Instruments = []
            for tk in tokens:
                wsToken = tk['wsToken']
                if wsToken != None : 
                    exch, tok = wsToken.split(", ")
                    Instruments.append({"exchangeSegment" : exch, 'exchangeInstrumentID' : int(tok)})
                else: 
                    notFound.append(tk['token'])
            
            if Instruments != [] : 
                response = self.xtconn.unsubscribe(Instruments, self.dType)
                if response.get('error') :
                    self.onError(f"Error in token Subscription : {response}")
            if notFound != [] : 
                self.onError(f"ERROR : Unable to find tokens for {notFound} ")
        except Exception as e : 
            self.onError(f"Error in token Unsubscription : {e}, Traceback : {traceback.format_exc()}")

    def on_message(self, data):
        pass
    
    def on_message1505_json_full(self, data):
        pass

    def on_message1507_json_full(self, data):
        pass

    def on_message1510_json_full(self, data):
        pass

    def on_message1512_json_full(self, data):
        pass

    def on_message1105_json_full(self, data):
        pass

    def on_message1501_json_partial(self, data):
        pass

    def on_message1502_json_partial(self, data):
        pass

    def on_message1505_json_partial(self, data):
        pass

    def on_message1510_json_partial(self, data):
        pass

    def on_message1512_json_partial(self, data):
        pass

    def on_message1105_json_partial(self, data):
        pass

    def closeConnection(self):
        self.run = False
        time.sleep(2)
        self.soc.sid.disconnect()
        self.onClose()

    def __connect(self):
        self.soc = XTSMDSocket_io(token=self.accountData['Sessionid'], userID=self.accountData['Clientid'],
                                  reconnection_attempts=5, URL=self.URL)
        self.soc.on_connect = self.onOpen
        self.soc.on_message = self.on_message
        self.soc.on_message1502_json_full = self.onDepthHandler
        self.soc.on_message1505_json_full = self.on_message1505_json_full
        self.soc.on_message1507_json_full = self.on_message1507_json_full
        self.soc.on_message1510_json_full = self.on_message1510_json_full
        self.soc.on_message1501_json_full = self.onLTPHandler
        self.soc.on_message1512_json_full = self.on_message1512_json_full
        self.soc.on_message1105_json_full = self.on_message1105_json_full
        self.soc.on_message1502_json_partial = self.on_message1502_json_partial
        self.soc.on_message1505_json_partial = self.on_message1505_json_partial
        self.soc.on_message1510_json_partial = self.on_message1510_json_partial
        self.soc.on_message1501_json_partial = self.on_message1501_json_partial
        self.soc.on_message1512_json_partial = self.on_message1512_json_partial
        self.soc.on_message1105_json_partial = self.on_message1105_json_partial
        self.soc.on_disconnect = self.onClose
        self.soc.on_error = self.onError
        el = self.soc.get_emitter()
        el.on('connect', self.onOpen)
        el.on('1501-json-full', self.onLTPHandler)
        el.on('1502-json-full', self.onDepthHandler)
        el.on('1507-json-full', self.on_message1507_json_full)
        el.on('1512-json-full', self.on_message1512_json_full)
        el.on('1105-json-full', self.on_message1105_json_full)
        try: 
            self.soc.connect()
        except Exception as e :  
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")
    
    def connect(self):
        self.xtconn = XTS_MarketData(sessionid = self.accountData['Sessionid'], URL=self.apiURL)
        threading.Thread(target=self.__connect).start()


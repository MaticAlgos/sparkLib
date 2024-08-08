import datetime
import threading
import json
import traceback
import queue
import time

from .dependencies import XTSMDSocket_io, XTS_MarketData

exchange_mapping = {
    "1" : "NSE",
    "2" : "NFO",
    "3" : "CDS",
    "11" : "BSE",
    "12" : "BFO"
}

resp_token_mapping = {
    "26001": "26009",
    "26034": "26037",
    "26121": "26074",
}

class IIFLXTSWS():
    tmdiff = (datetime.datetime(1980, 1, 1, 0, 0, 0) - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
    tokenmapping = {value: key for key, value in resp_token_mapping.items()}
    apiURL = 'https://ttblaze.iifl.com/apimarketdata' 
    URL = "https://ttblaze.iifl.com"
    Tokens = {}

    def __init__(self, accountData, dataType = "ltp", 
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

    def getToken(self, exch, token):
        try : 
            token = int(token)
            exc = exchange_mapping[str(exch)]
            if token in resp_token_mapping :
                token = resp_token_mapping[token]
            return exc + ":" + str(token)
        except : 
            self.onError(f"Exch : {exc}, Token : {token} received from XTSWS not found, Traceback : {traceback.format_exc()}")
            raise Exception("Exchange Name not found")
        
    def onLTPHandler(self, message):
        try: 
            data = json.loads(message)
            touchlineData = data if not data.get("Touchline") else data['Touchline']
            timestamp = self.tmdiff + touchlineData['LastUpdateTime']
            dt = datetime.datetime.utcfromtimestamp(timestamp)
            token = self.getToken(data['ExchangeSegment'], data['ExchangeInstrumentID'])
            msg = {"timestamp_str" : str(dt), 
                   "timestamp" : str(int(float(timestamp))),
                   "symbol" : token, 
                   "ltp" : touchlineData['LastTradedPrice'],
                   "prev_day_close" : touchlineData['Close'], 
                   "oi" : 0,
                   "prev_day_oi" : 0,
                   "turnover" : touchlineData['TotalTradedQuantity'], 
                   "best_bid_price" : touchlineData['BidInfo']['Price'], 
                   "best_ask_price" : touchlineData['AskInfo']['Price'], 
                   "best_bid_qty" : touchlineData['BidInfo']['Size'],
                   "best_ask_qty" : touchlineData['AskInfo']['Size'], 
                   "ttq" : touchlineData['TotalTradedQuantity'],  
                   "token" : token}
            self.onLtp(msg)
                
        except Exception as e : 
            self.onError(f"Error : {e}, Traceback : {traceback.format_exc()}")

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
            Instruments = []
            for i in tokens:
                if i not in self.Tokens.keys() : 
                    tk = i.split(":")
                    exch = 1 if tk[0] == "NSE" else 2 if tk[0] == "NFO" else "3" if tk[0] == "CDS" else 11 if tk[0] == "BSE" else 12 if tk[0] == "BFO" else None
                    token = int(tk[1])
                    if token in self.tokenmapping : 
                        token = self.tokenmapping[token]
                    if exch != None: 
                        self.Tokens[str(exch) + ":" + str(token)] = i
                        Instruments.append({"exchangeSegment" : exch, 'exchangeInstrumentID' : int(token)})
            if Instruments != [] : 
                response = self.xt.subscribe(Instruments, self.dType)
                if response.get('error') :
                    self.onError(f"Error in token Subscription : {response}")
        except Exception as e : 
            self.onError(f"Error in token Subscription : {e}, Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        try: 
            Instruments = []
            for i in tokens:
                tk = i.split(":")
                exch = 1 if tk[0] == "NSE" else 2 if tk[0] == "NFO" else "3" if tk[0] == "CDS" else 11 if tk[0] == "BSE" else 12 if tk[0] == "BFO" else None
                token = tk[1]
                if token in self.tokenmapping : 
                    token = self.tokenmapping[token]
                if exch != None: 
                    Instruments.append({"exchangeSegment" : exch, 'exchangeInstrumentID' : int(token)})
                if i in self.Tokens.keys():
                    self.Tokens.pop(i)

            if Instruments != [] : 
                response = self.xt.unsubscribe(Instruments, self.dType)
                if response.get('error') :
                    self.onError(f"Error in token Unsubscription : {response}")

        except Exception as e : 
            self.onError(f"Error in token Subscription : {e}, Traceback : {traceback.format_exc()}") 

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
        self.xt = XTS_MarketData(sessionid = self.accountData['Sessionid'], URL=self.apiURL)
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
            self.onError(f"Error : {e}, Traceback : {traceback.format_exc()}")
    
    def connect(self):
        threading.Thread(target=self.__connect).start()


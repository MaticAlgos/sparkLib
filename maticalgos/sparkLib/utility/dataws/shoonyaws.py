import datetime
import traceback
import os
from .dependencies import ShoonyaDataWS, SparkLib

class ShoonyaWS():
    dataStore = {}
    BROKER = "SHOONYA"
    TOKENS = {}
    def __init__(self, accountData, accessToken = None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) :
        self.accountData = accountData
        self.dType = 1 if dataType == "ltp" else 2 if dataType == "DepthUpdate" else None #Currently Only supports LTP Updates.
        self.onLtp = self.__onLTP if not onLtp  else onLtp
        self.onDepth = self.__onDepth if not onDepth else onDepth
        self.onError = self.__onError if not onError else onError
        self.onClose = self.__onClose if not onClose else onClose
        self.onOpen = self.__onOpen if not onOpen else onOpen
        self.run = True
        if not accessToken :
            if os.environ.get("MATICALGOS_AccessToken"):
                accessToken = os.environ["MATICALGOS_AccessToken"] 
            else: 
                raise Exception("Please generate access token.")
        self.spk = SparkLib(access_token=accessToken)
    
    def __onLTP(self, message):
        print(message)
    
    def __onDepth(self, message):
        print(message)
    
    def __onError(self, message):
        print("error : ",message)

    def __onClose(self, *args):
        print("Connection Closed")

    def __onOpen(self):
        print("Connection Opened")
    
    def getTokens(self, tokens):
        data = self.spk.getBrokerTokens(tokens=tokens, broker=self.BROKER)
        if data['status'] and not data['error'] : 
            tokens = data['data']
            return tokens
        else: 
            raise Exception (data['message'])
    
    def getSymbol(self, exchange, token):
        return self.TOKENS["|".join([exchange, token])]

    def tickHandler(self, message):
        if message['t'] in ['tk', 'tf'] : 
            try:
                data = {
                        "time" : message.get('ft'),
                        "bidPrice" : message.get('bp1') if message.get('bp1') != None else message.get('lp') if message.get('lp') != None else 0 ,
                        "askPrice" : message.get('sp') if message.get('sp') != None else message.get('lp') if message.get('lp') != None else 0,
                        "bidQty" : message.get('bq1') if message.get('bq1') != None else 0,
                        "askQty" : message.get('sq1') if message.get('sq1') != None else 0,
                        "token" : message.get('tk'),
                        "exchange" : message.get('e'), 
                        "ltp" : message.get('lp') if message.get('lp') != None else (float(message['bp1'])+float(message['sp1']))/2 if None not in [message.get('bp1'),message.get('sp1')] else None
                        }
                if None not in data.values() : 
                    token = self.getSymbol(message['e'], message['tk'])
                    dtime = datetime.datetime.fromtimestamp(int(data['time']))
                    openInterest = 0 if token.split(":")[0] not in ['NFO',"BFO","MCX"] else message.get('oi') if message.get('oi') != None else 0
                    if openInterest == None :
                        openInterest = self.dataStore[token]['oi'] if self.dataStore.get(token) != None else 0
                    volume = message.get('v') if message.get('v') != None else data[token]['v'] if data.get(token) != None else 0 
                    turnover = volume
                    prevClose = None
                    if self.dataStore.get(token) != None :
                        prevClose = self.dataStore[token]['c'] if self.dataStore[token].get("c") != None else None
                        
                    if not self.dataStore.get(token) or prevClose == None :  
                        prevClose = message.get('c') if message.get('c') != None else \
                                float(data['ltp'])/(1-(float(message.get('pc'))/100)) if message.get('pc') != None \
                                else 0.0
                                    
                    msg = {
                                    "timestamp_str" : str(dtime),
                                    "timestamp" : data['time'],
                                    "symbol" : token, 
                                    "ltp" : float(data['ltp']), 
                                    "prev_day_close" : prevClose, 
                                    "oi" : int(openInterest), 
                                    "prev_day_oi" : 0, 
                                    "turnover" : float(turnover) , 
                                    "best_bid_price" : float(data['bidPrice']), 
                                    "best_ask_price" : float(data['askPrice']), 
                                    "best_bid_qty" : int(data["bidQty"]),
                                    "best_ask_qty" : int(data['askQty']),
                                    "ttq" : int(volume),  
                                    "token" : token
                                    }
                    self.onLtp(msg)
                    self.dataStore[token] = {"c" : prevClose, 'oi' : openInterest, 'to' : turnover, "v" : volume}
                else: 
                    if None not in  [message.get('tk'),message.get('e')]:
                        token = self.getSymbol(message.get('e'), message.get('tk'))
                        prevclose = message.get('c') if message.get('c') != None \
                                    else float(message.get['lp'])/(1-(float(message.get('pc'))/100)) if (message.get('pc') != None and message.get('lp') != None) \
                                    else 0.0
                        data = {
                                "c" : prevclose,
                                "oi" : message.get('oi'),
                                "v" : message.get("v"),
                                'to' : message.get('v')
                            }
                        if self.dataStore.get(token) != None : 
                            data = {k : data[k] for k in data if data[k] != None }
                            self.dataStore[token].update(data)
                        else: 
                            data = {k : 0 for k in data if data[k] == None }
                            self.dataStore[token] = data
                            
            except Exception as e : 
                self.onError(f"Error : {e}, data : {message}, Traceback : {traceback.format_exc()}")

    def disconnect(self):
        self.run = False
        self.soc.close_websocket()
    
    def Subscribe(self, tokens):
        try: 
            tokens = self.getTokens(tokens)
            toSubs = []
            notFound = []
            for tk in tokens:
                wsToken = tk['wsToken'] 
                if wsToken != None : 
                    self.TOKENS[wsToken] = tk['token']
                    toSubs.append(wsToken)
                else: 
                    notFound.append(tk['token'])
            if toSubs != [] : 
                self.soc.subscribe(instrument=toSubs, feed_type=self.dType)
            if notFound != [] : 
                self.onError(f"Error : Unable to find tokens for {notFound} ")
        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        try: 
            tokens = self.getTokens(tokens)
            toSubs = []
            notFound = []
            for tk in tokens:
                wsToken = tk['wsToken'] 
                if wsToken != None : 
                    toSubs.append(wsToken)
                else: 
                    notFound.append(tk['token'])
            if toSubs != [] : 
                self.soc.unsubscribe(toSubs, feed_type=self.dType)
            if notFound != [] : 
                self.onError(f"Error : Unable to find tokens for {notFound} ")
        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")
            
    def connect(self):
        try: 
            sessionData = self.accountData['Sessionid']
            self.soc = ShoonyaDataWS(message_update_callback=self.tickHandler,
                   socket_open_callback=self.onOpen,
                   socket_close_callback=self.onClose,
                   socket_error_callback=self.onError,
                   susertoken=sessionData,
                   userid=self.accountData['Clientid'],
                   retry_attempts=4)
            self.soc.start_websocket()

        except Exception as e : 
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")

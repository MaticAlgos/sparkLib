import datetime
import traceback
from .dependencies import FyersDataSocket, SparkLib
import os 

class FyersWs():
    TOKENS = {}
    BROKER = "FYERS"

    def __init__(self, accountData, accessToken = None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) :
        self.accountData = accountData
        self.dType = "SymbolUpdate" if dataType == "ltp" else 1502 if dataType == "DepthUpdate" else None #Currently Only supports LTP Updates.
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
        self.Subscribe([])
        print("Connection Opened")
    
    def getTokens(self, tokens):
        data = self.spk.getBrokerTokens(tokens=tokens, broker=self.BROKER)
        if data['status'] and not data['error'] : 
            tokens = data['data']
            return tokens
        else: 
            raise Exception (data['message'])
        
    def getSymbol(self, symbol):
        return self.TOKENS[symbol]

    def tickHandler(self, message):
        try: 
            if message.get("type") in ['if', "sf"] : 
                token = self.getSymbol(message['symbol']) #self.Tokens[message['symbol']]
                msg = {"timestamp_str" : str(datetime.datetime.fromtimestamp(message['exch_feed_time'])),
                       "timestamp" : str(message['exch_feed_time']),
                       "symbol" : token, 
                       "ltp" : float(message['ltp']), 
                       "prev_day_close" : float(message['prev_close_price']), 
                       "oi" : 0, 
                       "prev_day_oi" : 0, 
                       "turnover" : int(message['vol_traded_today']) if message.get('vol_traded_today') != None else 0, 
                       "best_bid_price" : float(message['bid_price']) if message.get('bid_price') != None else 0, 
                       "best_ask_price" : float(message['ask_price']) if message.get('ask_price') != None else 0, 
                       "best_bid_qty" : int(message['bid_size']) if message.get('bid_size') != None else 0,
                       "best_ask_qty" : int(message['ask_size']) if message.get('ask_size') != None else 0, 
                       "ttq" : int(message['vol_traded_today']) if message.get('vol_traded_today') != None else 0,  
                       "token" : token}
                self.onLtp(msg)
        except Exception as e : 
            self.onError(f"Error : {e}, Traceback : {traceback.format_exc()}")

    def disconnect(self):
        self.run = False
        self.fyers.close_connection()
    
    def Subscribe(self, tokens):
        try: 
            toSubs = []
            notFound = []
            tokens = self.getTokens(tokens)
            for tk in tokens :
                wsToken = tk['wsToken']
                if wsToken != None : 
                    toSubs.append(wsToken)
                    self.TOKENS[wsToken] = tk["token"]
                else: 
                    notFound.append(tk['token'])
            
            if toSubs != [] : 
                self.fyers.subscribe(symbols=toSubs, data_type=self.dType)

            if notFound != [] : 
                self.onError(f"ERROR : Unable to find tokens for {notFound} ")
        except Exception as e: 
            self.onError(f"Error : {e}. Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        try: 
            toUnsubs = []
            notFound = []
            tokens = self.getTokens(tokens)
            for tk in tokens :
                wsToken = tk['wsToken']
                if wsToken != None : 
                    toUnsubs.append(wsToken)
                else: 
                    notFound.append(tk['token'])

            if toUnsubs != [] : 
                self.fyers.unsubscribe(symbols=toUnsubs, data_type=self.dataType)

            if notFound != [] : 
                self.onError(f"ERROR : Unable to find tokens for {notFound} ")
                
        except: 
            self.onError("Error with Unsubscribe")

    def connect(self):
        try: 
            self.fyers = FyersDataSocket(
                access_token=self.accountData["Sessionid"],       
                log_path="",                    
                litemode=False,                  
                write_to_file=False,              
                reconnect=True,                  
                on_connect=self.onOpen,             
                on_close=self.onClose,             
                on_error=self.onError,
                on_message=self.tickHandler,
            )
            self.fyers.connect()
        except Exception as e : 
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")

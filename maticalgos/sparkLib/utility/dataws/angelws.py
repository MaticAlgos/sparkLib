import datetime
import traceback
import os
from .dependencies import SmartWebSocketV2, SparkLib

class AngelWs():
    correlation_id = "abcd1234"
    BROKER = "ANGELONE"
    TOKENS = {}
    def __init__(self, accountData, accessToken= None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) :
        self.accountData = accountData
        self.dType = 3 if dataType == "ltp" else 3 if dataType == "DepthUpdate" else None #Currently Only supports LTP Updates.
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
        return self.TOKENS[", ".join([str(exchange), str(token)])]
    
    def tickHandler(self, message):
        try: 
            token = self.getSymbol(message['exchange_type'], message['token'])
            ts = int(message['exchange_timestamp']/1000) #message['last_traded_timestamp'] if str(message['last_traded_timestamp']) != "0" else 
            msg = {"timestamp_str" : str(datetime.datetime.fromtimestamp(ts)),
                "timestamp" : str(ts),
                "symbol" : token, 
                "ltp" : float(message['last_traded_price']/100), 
                "prev_day_close" : float(message['closed_price']/100), 
                "oi" : int(message['open_interest']), 
                "prev_day_oi" : 0, 
                "turnover" : int(message['volume_trade_for_the_day']) , 
                "best_bid_price" : float(message['best_5_buy_data'][0]['price']/100) if message.get('best_5_buy_data') else 0, 
                "best_ask_price" : float(message['best_5_sell_data'][-1]['price']/100) if message.get('best_5_sell_data') else 0, 
                "best_bid_qty" : int(message['best_5_buy_data'][0]['quantity']) if message.get('best_5_buy_data') else 0,
                "best_ask_qty" : int(message['best_5_sell_data'][-1]['quantity']) if message.get('best_5_sell_data') else 0, 
                "ttq" : int(message['volume_trade_for_the_day']),  
                "token" : token}
            self.onLtp(msg)
        except Exception as e : 
            self.onError(f"Error : {e}, Traceback : {traceback.format_exc()}")

    def disconnect(self):
        self.run = False
        self.ang.close_connection()
    
    def Subscribe(self, tokens):
        try: 
            tokens = self.getTokens(tokens)
            tkManage = {}
            notFoundTK = []
            
            for tk in tokens:
                wsToken = tk['wsToken'] 
                if wsToken != None : 
                    exch, tok = wsToken.split(", ")
                    if not tkManage.get(exch) : 
                        tkManage[exch] = {"action" : 1, "tokens" : [], "exchangeType" : exch}
                    tkManage[exch]['tokens'].append(tok)
                    self.TOKENS[wsToken] = tk['token']
                else: 
                    notFoundTK.append(tk['token'])

            lst = [tkManage[i] for i in tkManage]
            self.ang.subscribe(correlation_id=self.correlation_id, mode=self.dType, token_list=lst)
            if notFoundTK != [] : 
                self.onError(f"ERROR : Unable to find tokens for {notFoundTK}")

        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        tkManage = {}
        try: 
            tokens = self.getTokens(tokens)
            for tk in tokens: 
                wsToken = tk['wsToken']
                if wsToken != None : 
                    exch, tok = wsToken.split(", ")
                    if not tkManage.get(exch) : 
                        tkManage[exch] = {"action" : 0, "tokens" : [], "exchangeType" : int(exch)}
                    tkManage[exch]['tokens'].append(tok[3:])

            lst = [tkManage[i] for i in tkManage]
            self.ang.unsubscribe(correlation_id=self.correlation_id, mode=self.dType, token_list=lst)

        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")
            
    def connect(self):
        try: 
            sessionData = self.accountData['Sessionid']
            self.ang = SmartWebSocketV2(auth_token=sessionData['jwtToken'],
                                        api_key=self.accountData['ApiKey'],
                                        client_code=self.accountData['Clientid'],
                                        feed_token=sessionData['feedToken'],
                                        max_retry_attempt=5,
                                        on_Message=self.tickHandler,
                                        on_Error=self.onError,
                                        on_Close=self.onClose,
                                        on_Open=self.onOpen)
            
            self.ang.connect()
        except Exception as e : 
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")

import datetime
import traceback
import json 
import os
from .dependencies import KotakDataFeed, SparkLib

class Kotakws():
    dataStore = {}
    BROKER = "KOTAKNEO"
    TOKENS = {}
    def __init__(self, accountData, accessToken = None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) :
        self.accountData = accountData
        self.dType = False if dataType == "ltp" else True if dataType == "DepthUpdate" else None #Currently Only supports LTP Updates.
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
        return self.TOKENS[", ".join([exchange, token])]

    def tickHandler(self, message):
        try:
            if message.get('data') != None : 
                for dt in message['data'] : 
                    if dt['name'] == "sf" : 
                        data = {
                            "time":dt.get('fdtm') if dt.get('fdtm') != None else dt.get("ltt"),
                            "bidPrice" : dt.get('bp') if dt.get('bp') != None else dt.get('ltp') if dt.get('ltp') != None else 0 ,
                            "askPrice" : dt.get('sp') if dt.get('sp') != None else dt.get('ltp') if dt.get('ltp') != None else 0,
                            "bidQty" : dt.get('bq') if dt.get('bq') != None else 0,
                            "askQty" : dt.get('bs') if dt.get('bs') != None else 0,
                            "token" : dt.get('tk'),
                            "exchange" : dt.get('e'),
                            "ltp" : dt.get('ltp') if dt.get('ltp') != None else (float(dt['bp'])+float(dt['sp']))/2 if None not in [dt.get('bp'),dt.get('sp')] else None
                                }
                        if None not in data.values() : 
                            dtime = datetime.datetime.strptime(data['time'], "%d/%m/%Y %H:%M:%S")
                            token = self.getSymbol(data['exchange'], data['token'])
                            openInterest = 0 if token.split(":")[0] not in ['NFO',"BFO","MCX"]  else dt.get('oi') if dt.get('oi') != None else None
                            if openInterest == None :
                                try:
                                    openInterest = self.dataStore[token]['oi'] if self.dataStore.get(token) != None else 0
                                except:
                                    openInterest = 0
                            turnover = dt.get('to') if dt.get('to') != None else data[token]['to'] if data.get(token) != None else 0 
                            volume = dt.get('v') if dt.get('v') != None else data[token]['v'] if data.get(token) != None else 0 
                            
                            if self.dataStore.get(token) != None :
                                try: 
                                    prevClose = self.dataStore[token]['c']
                                except:
                                    prevClose = 0.0
                            else: 
                                prevClose = dt.get('c') if dt.get('c') != None else float(data['ltp'])-float(dt.get('cng')) if dt.get('cng') != None else 0.0
                
                            msg = {
                                "timestamp_str" : str(dtime),
                                "timestamp" : str(int(dtime.timestamp())),
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
                            if None not in  [dt.get('tk'),dt.get('e')]:
                                token = self.getSymbol(dt.get('e'), dt.get('tk'))
                                prevclose = dt.get('c') if dt.get('c') != None else float(dt.get('ltp'))-float(dt.get('cng')) if (dt.get('cng') != None and dt.get("ltp") != None) else None
                                data = {
                                        "c" : prevclose,
                                        "oi" : dt.get('oi'),
                                        "v" : dt.get("v") ,
                                        'to' : dt.get('to')
                                    }
                                if self.dataStore.get(token) != None : 
                                    data = {k : data[k] for k in data if data[k] != None }
                                    self.dataStore[token].update(data)
                                else: 
                                    data = {k : 0 for k in data if data[k] == None }
                                    self.dataStore[token] = data

                    elif dt['name'] == 'if' : 
                        data = {
                            "time":dt.get('tvalue'),
                            "volume": 0,
                            "ltp" : dt.get('iv'),
                            "bidPrice" : 0,
                            "askPrice" : 0,
                            "bidQty" : 0,
                            "askQty" : 0,
                            "turnOver" : 0,
                            "token" : dt.get('tk'),
                            "exchange" : dt.get('e'),
                                }
                        if None not in data.values() : 
                            dtime = datetime.datetime.strptime(data['time'], "%d/%m/%Y %H:%M:%S")
                            token = self.getSymbol(data['exchange'], data['token'])
                            prevClose = dt.get('ic') if dt.get('ic') != None else float(data["ltp"])-float(dt.get('cng')) if dt.get('cng') == None else None
                            if prevClose == None and self.dataStore.get(token) != None :
                                prevClose = self.dataStore[token]['c']
                            else: 
                                prevClose = 0.0

                            msg = {
                                    "timestamp_str" : str(dtime),
                                    "timestamp" : str(int(dtime.timestamp())),
                                    "symbol" : token, 
                                    "ltp" : float(data['ltp']), 
                                    "prev_day_close" : prevClose, 
                                    "oi" : 0, 
                                    "prev_day_oi" : 0, 
                                    "turnover" : data['turnOver'] , 
                                    "best_bid_price" : data['bidPrice'], 
                                    "best_ask_price" : data['askPrice'], 
                                    "best_bid_qty" : data["bidQty"],
                                    "best_ask_qty" : data['askQty'],
                                    "ttq" : data['volume'],  
                                    "token" : token
                                    }
                                
                            self.dataStore[token] = {"c" : prevClose, 'oi' : 0}
                            self.onLtp(msg)
                        
        except Exception as e : 
            self.onError(f"Error : {e}, data : {message}, Traceback : {traceback.format_exc()}")

    def disconnect(self):
        self.run = False
        self.soc.close_websocket()
    
    def chunk_list(self, data, chunk_size):
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    def Subscribe(self, tokens):
        try: 
            tokens = self.getTokens(tokens)
            toSubs = []
            toSubsIndex = []
            notFound = []
            for tk in tokens:
                wsToken = tk['wsToken'] 
                if wsToken != None : 
                    exch, wstk = wsToken.split(", ")
                    self.TOKENS[wsToken] = tk['token']
                    if tk['is_index'] :
                        toSubsIndex.append({"instrument_token" : wstk, "exchange_segment" : exch})
                    else: 
                        toSubs.append({"instrument_token" : wstk, "exchange_segment" : exch})
                else: 
                    notFound.append(tk['token'])

            if toSubs != [] : 
                chunked_list = self.chunk_list(toSubs,99)
                for lst in chunked_list:
                    self.soc.get_live_feed(lst, isIndex=False, isDepth=False)
            if toSubsIndex != [] : 
                chunked_list = self.chunk_list(toSubsIndex,99)
                for lst in chunked_list:
                    self.soc.get_live_feed(lst, isIndex=True, isDepth=False) 
            if notFound != [] : 
                self.onError(f"Error : Unable to find tokens for {notFound} ")

        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

    def Unsubscribe(self, tokens):
        try: 
            tokens = self.getTokens(tokens)
            toSubs = []
            toSubsIndex = []
            notFound = []
            for tk in tokens:
                wsToken = tk['wsToken'] 
                if wsToken != None : 
                    exch, wstk = wsToken.split(", ")
                    if tk['is_index'] :
                        toSubsIndex.append({"instrument_token" : wstk, "exchange_segment" : exch})
                    else: 
                        toSubs.append({"instrument_token" : wstk, "exchange_segment" : exch})
                else: 
                    notFound.append(tk['token'])

            if toSubs != [] : 
                chunked_list = self.chunk_list(toSubs,99)
                for lst in chunked_list:
                    self.soc.un_subscribe_list(lst, isIndex=False, isDepth=False)
            if toSubsIndex != [] : 
                chunked_list = self.chunk_list(toSubsIndex,99)
                for lst in chunked_list:
                    self.soc.un_subscribe_list(lst, isIndex=True, isDepth=False) 
            if notFound != [] : 
                self.onError(f"Error : Unable to find tokens for {notFound} ")

        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

    def connect(self):
        try: 
            sessionData = self.accountData['Sessionid']
            sessionData = sessionData if type(sessionData) == dict else json.loads(sessionData)
            self.soc = KotakDataFeed(token=sessionData['token'],
                                     sid=sessionData['sid'],
                                     server_id=sessionData['hsServerId'],
                                     on_message=self.tickHandler,
                                     on_close=self.onClose,
                                     on_error=self.onError,
                                     on_open=self.onOpen,
                                     retry_attempts=5)
            self.soc.start_websocket()

        except Exception as e : 
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")

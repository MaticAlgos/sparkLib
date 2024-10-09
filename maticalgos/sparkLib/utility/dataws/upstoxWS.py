import datetime
import traceback
import asyncio
import os 
import queue
from .dependencies import AsyncUpstockOrderUpdate
from .dependencies import SparkLib

class UpstoxWS():
    BROKER = "UPSTOX"
    TOKENS = {}
    
    def __init__(self, accountData, accessToken = None, dataType = "ltp", 
                 onLtp = None, 
                 onDepth = None, 
                 onError = None, 
                 onClose = None,
                 onOpen = None,
                 ) : 
        self.accountData = accountData
        self.dType = "full"  # if dataType == "ltp" else "full" if dataType == "DepthUpdate" else None #Currently Only supports LTP Updates.
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

    async def on_message(self, message):
        self.tickHandler(message)

    async def on_connect(self):
        self.onOpen()
        # await client.subscribe("ltpc", ['NSE_FO|36805'])

    async def on_error(self, error):
        print("WebSocket error:", error)
        self.onError(error)

    async def on_close(self, code, reason):
        self.onClose(code, reason)

    def getTokens(self, tokens):
        data = self.spk.getBrokerTokens(tokens=tokens, broker=self.BROKER)
        if data['status'] and not data['error'] : 
            tokens = data['data']
            return tokens
        else: 
            raise Exception (data['message'])
    
    def getSymbol(self, token):
        return self.TOKENS[token]
    
    def tickHandler(self, message):
        for key in message['feeds'].keys():
            try: 
                token = self.getSymbol(key)
                data = message['feeds'][key]['ff']
                dKey = next(iter(data))
                if dKey == "indexFF" : 
                    tickData = data[dKey]
                    msg = {
                            "timestamp_str" : str(datetime.datetime.fromtimestamp(int(tickData['ltpc']['ltt'])/1000)),
                            "timestamp" :tickData['ltpc']['ltt'],
                            "symbol" : token, 
                            "ltp" : float(tickData['ltpc']['ltp']), 
                            "prev_day_close" : float(tickData['ltpc']['cp']), 
                            "oi" : 0, 
                            "prev_day_oi" : 0, 
                            "turnover" : 0 , 
                            "best_bid_price" : 0, 
                            "best_ask_price" : 0, 
                            "best_bid_qty" : 0,
                            "best_ask_qty" : 0,
                            "ttq" : 0,  
                            "token" : token
                                        }
                    self.onLtp(msg)
                elif dKey == "marketFF":
                    tickData = data[dKey]
                    msg = {
                            "timestamp_str" : str(datetime.datetime.fromtimestamp(int(tickData['ltpc']['ltt'])/1000)),
                            "timestamp" :tickData['ltpc']['ltt'],
                            "symbol" : token, 
                            "ltp" : float(tickData['ltpc']['ltp']), 
                            "prev_day_close" : float(tickData['ltpc']['cp']), 
                            "oi" : int(tickData['eFeedDetails']['oi']) if tickData['eFeedDetails'].get('oi') else 0, 
                            "prev_day_oi" : int(tickData['eFeedDetails']['poi']) if tickData['eFeedDetails'].get('poi') else 0, 
                            "turnover" : int(tickData['eFeedDetails']['vtt']) if tickData['eFeedDetails'].get('vtt') else 0, 
                            "best_bid_price" : float(tickData['marketLevel']['bidAskQuote'][0]['bp']), 
                            "best_ask_price" : float(tickData['marketLevel']['bidAskQuote'][0]['ap']), 
                            "best_bid_qty" : float(tickData['marketLevel']['bidAskQuote'][0]['bq']),
                            "best_ask_qty" : float(tickData['marketLevel']['bidAskQuote'][0]['aq']),
                            "ttq" : int(tickData['eFeedDetails']['vtt']) if tickData['eFeedDetails'].get('vtt') else 0,  
                            "token" : token
                                        }
                    self.onLtp(msg)
            except Exception as e :
                self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

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
                self.subsQueue.put_nowait({'func':"sub","token":toSubs})
                # self.client.subscribe(self.dType, toSubs)
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
                self.subsQueue.put_nowait({'func':"unsub","token":toSubs})
                # self.client.unsubscribe(self.dType, toSubs)
            if notFound != [] : 
                self.onError(f"Error : Unable to find tokens for {notFound} ")

        except Exception as e :
            self.onError(f"Error : {str(e)}, Traceback : {traceback.format_exc()}")

    def disconnect(self):
        self.run = False

    async def _connect(self):
        try : 
            self.client = AsyncUpstockOrderUpdate(
                                                auth_token=self.accountData['Sessionid'],
                                                on_message=self.on_message,
                                                on_connect=self.on_connect,
                                                on_error=self.on_error,
                                                on_close=self.on_close
                                            )
            asyncio.create_task(self.client.connect())
            while self.run : 
                if not self.subsQueue.empty():
                    data = await self.subsQueue.get()
                    if data['func'] == "sub": 
                        await self.client.subscribe(self.dType, data['token'])
                    elif data['func'] == "unsub":
                        await self.client.unsubscribe(self.dType, data['token'])

                await asyncio.sleep(1)
            await self.client.close_connection()
        
        except Exception as e : 
            self.onClose(f"Error : {e}, Traceback : {traceback.format_exc()}")

    def connect(self): 
        self.subsQueue = asyncio.Queue()
        asyncio.run(self._connect())

import zmq
import time 
import json 
import traceback
import threading
import datetime
import redis
import pandas as pd
import duckdb
import queue

class wsLib():
    def __init__(self, host = "localhost", ports = ['8542']):
        self.host = host
        self.ports = ports
        self.__connect()

    def __connect(self):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt( zmq.LINGER,100)
        socket.setsockopt( zmq.AFFINITY,1) 
        socket.setsockopt( zmq.RCVTIMEO, 20000) # 20 Seconds timeout

        for port in self.ports:
            socket.connect ("tcp://{host}:{port}".format(host = self.host, port = port))
        return socket
    
    def connect(self, AccountName, connectionType = "ltp"):
        """
        connectionType: str, values : 'ltp' or 'depth'
        """
        msg = {"function": "startWS", "payload" : {"AccountName" : AccountName, "connectionType" : connectionType}}
        return self._request(msg)
    
    def reconnect(self, AccountName):
        msg = {"function": "reconnectWs", "payload" : {"AccountName" : AccountName}}
        return self._request(msg)
    
    def stop(self, AccountName):
        msg = {"function": "stopWs", "payload" : {"AccountName" : AccountName}}
        return self._request(msg)
    
    def subscribe(self, AccountName, tokens:list):
        msg = {"function": "subscribe", "payload" : {"AccountName" : AccountName, "tokens" : tokens}}
        return self._request(msg)
    
    def unsubscribe(self, AccountName, tokens:list):
        msg = {"function": "unsubscribe", "payload" : {"AccountName" : AccountName, "tokens" : tokens}}
        return self._request(msg)
    
    def connections(self):
        msg = {"function":"connections", "payload" : {}}
        return self._request(msg)
    
    def _request(self, msg):
        socket = self.__connect()
        try : 
            socket.send(json.dumps(msg).encode())
            response = json.loads(socket.recv().decode())
            return response
        
        except zmq.Again:
            raise Exception("Timeout")
        except zmq.ZMQError:
            raise Exception("Timeout")
        except Exception as e : 
            print(traceback.print_exc())
            raise Exception(e)

import ast

class dataStream():
    _run = True
    tokens = []
    __allTicks = False
    __allMin = False
    timeout = 2.5
    MAX_RECONNECT = 10
    RECONNECT_NO = 0
    isReconnecting = False
    __thRunner = {}
    
    def __init__(self, host = "localhost", port = 8543):
        self.host = host
        self.port = port
        self.tickStream = self.__tickStream
        self.minStream = self.__minStream
        self.lastUpdate = time.time()
        self.lock = threading.Lock()
        self.ReconQueue = queue.Queue()
        self.is_recon = False
        self.maxReconAlert = False
        
    def __tickStream(self, message):
        print(message)
        
    def __minStream(self, message):
        print(message)
    
    # def __connect(self):
    #     try: 
    #         while self._run : 
    #             try : 
    #                 data = self.socket.recv(zmq.NOBLOCK) #flags = zmq.NOBLOCKS
    #                 # print(data)
    #                 # token, message = data.decode().split(" _&_ ")
    #                 # if token in self.tokens : 
    #                 #     dt = json.loads(message)
    #                 #     self.__updateHandler(message = dt, isTick = True if "tick" in token else False)
                    
    #                 # msgpack
    #                 # message = msgpack.unpackb(data)
    #                 # token = message['token']
    #                 # if token in self.tokens : 
    #                 #     self.__updateHandler(message = message, isTick = True if message['type'] == "tick" else False)

    #                 data = ast.literal_eval(data.decode())
    #                 for d in data : 
    #                     token, message = d.split(" _&_ ")
    #                     if token in self.tokens or ("tick" in token and self.__allTicks) or ("min" in token and self.__allMin) : 
    #                         dt = json.loads(message)
    #                         self.__updateHandler(message = dt, isTick = True if "tick" in token else False)
                
    #             except zmq.Again : 
    #                 time.sleep(0.01)
    #                 pass
                
    #             except zmq.ZMQError as e : 
    #                 traceback.print_exc()
    #                 print(e)
    #                 self._run = False

    #             except Exception as e:
    #                 traceback.print_exc()
    #                 print(f"Unexpected error: {e}")
    #                 self._run = False
                    
    #     finally: pass
    
    def __connect(self, key = None):
        try: 
            self.context = zmq.Context().instance()
            self.socket = self.context.socket(zmq.SUB)
            self.socket.setsockopt(zmq.LINGER, 100)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
            self.socket.connect("tcp://{host}:{port}".format(host = self.host, port = self.port))
            self.poller = zmq.Poller()
            self.poller.register(self.socket, zmq.POLLIN)
            key = int(time.time())
            self.__thRunner[key] = True
            print(self.__thRunner, key)
            while  self.__thRunner[key] : 
                try : 
                    events = dict(self.poller.poll(timeout=100))
                    if events : 
                        self.lastUpdate = time.time()
                        if self.socket in events : 
                            data = self.socket.recv(zmq.NOBLOCK) #flags = zmq.NOBLOCKS
                            data = ast.literal_eval(data.decode())
                            for d in data : 
                                token, message = d.split(" _&_ ")
                                if token in self.tokens or ("tick" in token and self.__allTicks) or ("min" in token and self.__allMin) : 
                                    dt = json.loads(message)
                                    self.__updateHandler(message = dt, isTick = True if "tick" in token else False)
                    else: 
                        if time.time() - self.lastUpdate > self.timeout and self.tokens != [] and not self.isReconnecting : 
                            if self.RECONNECT_NO < self.MAX_RECONNECT : 
                                print("RECON", self.RECONNECT_NO)
                                self.RECONNECT_NO += 1 
                                with self.lock : 
                                    self.__thRunner[key] = False
                                    self.isReconnecting = True
                            elif not self.maxReconAlert: 
                                print(f"We've reached max reconnections of {self.MAX_RECONNECT}, There may be some issue with the data feed.") 
                                self.maxReconAlert = True
                except zmq.ZMQError as e : 
                    traceback.print_exc()
                    print(e)
                    self.__thRunner[key] = False

                except Exception as e:
                    traceback.print_exc()
                    print(f"Unexpected error: {e}")
                    self.__thRunner[key] = False
                    
            if not self.__thRunner[key] : 
                if self.isReconnecting:
                    self.ReconQueue.put(key)
        
        finally:
            pass 
    
    def reconQueue(self):
        while self._run:
            try: 
                data = self.ReconQueue.get(timeout=1)
                self.stop(restart=True, key=data) 
                time.sleep(5)
                self.connect()
                with self.lock:
                    self.isReconnecting = False
                    
                time.sleep(0.1)
            except queue.Empty:
                pass
    
    def stop(self, restart = False, key = None):
        if not restart: 
            self._run = False
            with self.lock:
                for key in self.__thRunner.keys():
                    self.__thRunner[key] = False
            time.sleep(1)
        with self.lock:
            if key != None : 
                self.__thRunner[key] = False
        try:   
            self.poller.unregister(self.socket)
        except:
            pass
        self.socket.close()
        self.context.term()

    def connect(self):
        if not self.is_recon:
            threading.Thread(target=self.reconQueue).start()
            self.is_recon = True
        th = threading.Thread(target = self.__connect, daemon=True).start()

    def reconnect(self):
        self.RECONNECT_NO+=1 
        self.stop()
        with self.lock:
            while any(self.__thRunner.values()):
                time.sleep(0.1)
        self.connect()
        with self.lock:
            self.isReconnecting = False

    def subscribeAll(self, ticks = True):
        if ticks: 
            self.__allTicks = True
        else: 
            self.__allMin = True
            self.tokens.append("min:*")

    def subscribe(self, tokens:list, isTick = True):
        for t in tokens : 
            tok = f"min:{t}" if not isTick else f"tick:{t}"
            self.socket.setsockopt_string(zmq.SUBSCRIBE, tok)
            self.tokens.append(tok)
    
    def unsubscribe(self, tokens:list, isTick = True):
        for t in tokens : 
            tok = f"min:{t}" if not isTick else f"tick:{t}"
            self.socket.setsockopt_string(zmq.UNSUBSCRIBE, tok)
            self.tokens.remove(tok)
           
    def __updateHandler(self, message, isTick = True):
        if isTick : 
            self.tickStream(message)
        else: 
            self.minStream(message)

class orderwsStream():
    _run = True
    
    def __init__(self, host = "localhost", port = 8544):
        self.host = host
        self.port = port
        self.__ZMQcontext = zmq.Context()
        self.socket = self.__ZMQcontext.socket(zmq.SUB)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.updates = self.__wsUpdates
    
    def __wsUpdates(self, message):
        print(message)
    
    def __connect(self):
        self.socket.connect("tcp://{host}:{port}".format(host = self.host, port = self.port))
        while self._run : 
            try : 
                data = self.socket.recv() #flags = zmq.NOBLOCKS
                data = json.loads(data.decode())
                self.updates(data)
            except : 
                traceback.print_exc()
            time.sleep(0.01)
    
    def stop(self):
        self._run = False
        self.socket.close()
        self.__ZMQcontext.term()
        self.__ZMQcontext.destroy()
    
    def connect(self):
        threading.Thread(target = self.__connect).start()

class redisData():
    defaultStart = datetime.datetime.now().replace(hour=9,minute=15,second=0,microsecond=0)
    defaultEnd = datetime.datetime.now().replace(hour=15,minute=30,second=0,microsecond=0)
    def __init__(self, host = "localhost", port = 6379, password=None):
        self.host = host
        self.port = port
        self.password = password
        self.__pool()
    
    def __pool(self):
        self.pool = redis.ConnectionPool(host = self.host, port = self.port)
    
    def __connect(self):
        return redis.Redis(connection_pool=self.pool,password=self.password)
     
    def getMin(self, token, starttime = "", endtime = "", timeframe = 1, origin = datetime.time(9,15)):
        starttime = self.defaultStart if starttime == "" else starttime
        endtime = self.defaultEnd if endtime == "" else endtime

        start_ts = int((starttime).timestamp())
        end_ts = int((endtime).timestamp())
        r = self.__connect()
        data = r.zrangebyscore(f"min:{token}", start_ts, end_ts)
        data = [json.loads(i.decode()) for i in data]
        if data == [] : 
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df = df[['ts','tk','o','h','l','c','v','oi']]
        df = df.rename(columns={"ts":"datetime",'tk':'token',"o":"open","h":'high','l':"low","c":"close","v":"volume"})
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H:%M:%S")
        df = df.set_index('datetime')
        initVal = df['volume'].iloc[0]
        df[['volume']] = df[['volume']].fillna(0)
        df["volume"] = df['volume'] - df["volume"].shift(1, fill_value = 0)
        df.loc[df.index[0], 'volume'] = initVal

        if timeframe != 1 : 
            df = df.resample(f'{str(timeframe)}min', origin = datetime.datetime.combine(df.iloc[0].name.date(), origin)).agg({"open" : "first", 
                                                          "token" : "last",
                                                          "high" : "max", 
                                                          "low" : "min", 
                                                          "close" : "last", 
                                                          "volume" : "sum", 
                                                          "oi" : "last"
                                                          }).dropna()
        return df
    
    def getMinCandle(self, token, dt):
        r = self.__connect()
        ts = int(dt.timestamp())
        data = r.zrangebyscore(f"min:{token}", ts, ts)
        if data == [] : 
            return None
        return json.loads(data[0].decode())


class histDB():
    def __init__(self, filename, path:str="./"):
        self.filename=filename
        self.path=path
        self.startTime= datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)-datetime.timedelta(days=1)
        self.endTime = datetime.datetime.now().replace(hour=23,minute=0,second=0,microsecond=0)-datetime.timedelta(days=1)
        self.columns = "token,datetime,open,high,low,close,volume,oi"
    def __connect(self):
        return duckdb.connect(r"{path}/{filename}.db".format(path=self.path,filename=self.filename), read_only=True)

    def fetchData(self, token, startTime:datetime=None, endTime:datetime=None, timeframe:int=1, origin = datetime.time(9,15)):
        conn = self.__connect()
        startTime = self.startTime if startTime == None else startTime
        endTime = self.endTime if endTime == None else endTime
        df =  conn.execute(f"""SELECT {self.columns} FROM dbtable WHERE token='{token}' AND datetime >= '{str(startTime)}' AND datetime <= '{str(endTime)}' ORDER BY datetime ASC""").df()
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H:%M:%S")
        df = df.set_index('datetime')
        if timeframe != 1 : 
            df = df.resample(f'{str(timeframe)}min', origin = datetime.datetime.combine(df.iloc[0].name.date(), origin)).agg({"open" : "first", 
                                                          "high" : "max", 
                                                          "low" : "min", 
                                                          "close" : "last", 
                                                          "volume" : "sum", 
                                                          "token" : "last",
                                                          "oi" : "last"
                                                          }).dropna()
        return df

class tradingHistData():
    def __init__(self, redisconf = {"host":"localhost","port":"6379","password":None},
                       historical = False, filename:str=None, path:str="./"):
        self.historical=False
        if historical:
            self.hist = histDB(filename=filename,path=path)
            self.historical = True
        self.redisData = redisData(**redisconf)
        self.startTime= datetime.datetime.now().replace(hour=9,minute=15,second=0,microsecond=0)
        self.endTime = datetime.datetime.now().replace(hour=15,minute=30,second=0,microsecond=0)

    def _resample(self, df, timeframe, origin = datetime.time(9,15)):
        df = df.resample(f'{str(timeframe)}min', origin = datetime.datetime.combine(df.iloc[0].name.date(), origin)).agg({"open" : "first", 
                                                          "high" : "max", 
                                                          "low" : "min", 
                                                          "close" : "last", 
                                                          "volume" : "sum", 
                                                          "token" : "last",
                                                          "oi" : "last"
                                                          }).dropna()
        return df 
    
    def fetchData(self, token:str,startTime:datetime=None,endTime:datetime=None, timeframe:int=1):
        startTime = self.startTime if startTime == None else startTime
        endTime = self.endTime if endTime == None else endTime
        if startTime < self.startTime and self.historical:
            data = self.hist.fetchData(token=token,startTime=startTime,
                                       endTime=endTime, timeframe=timeframe)
        else: 
            data = pd.DataFrame()
            
        if endTime > self.startTime:
            dataTd = self.redisData.getMin(token=token, starttime=startTime,endtime=endTime,timeframe=timeframe)
        else: 
            dataTd = pd.DataFrame()

        if not data.empty and not dataTd.empty : 
            master = pd.concat([data,dataTd])
            return master
        
        if not data.empty: return data
        if not dataTd.empty: return dataTd



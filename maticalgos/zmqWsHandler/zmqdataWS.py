from multiprocessing import Process, Queue
import zmq
from functools import wraps
import logging 
import datetime
import json 
import time 
import traceback 
import copy
import threading
import queue 
import msgpack
# import SparkLib
from maticalgos import SparkLib,OrderSocket, dataws

BROKERS = {
    "IIFLXTSDATA" : dataws.IIFLXTSWS,
}

BROKERconf = {
    "IIFLXTSDATA" : {"maxConnection" : 1, "tokenLimit" : 800, "OI" : False, 
                     "is_depth" : True, "segments" : ['NSE', 'BSE', "NFO", "BFO"]}
}

class OrderWS():
    def __init__(self, sessionid, ordPubSub, logger) : 
        pass


class brokerWS():
    def __init__(self, accountData, respQueue, subsQueue, connectionType = "ltp") :
        self.accountData = accountData
        self.identifier = ":".join([self.accountData['UCC'], self.accountData['AccountName']])
        self._function = BROKERS[self.accountData['Broker']]
        self.broker = self.accountData['Broker']
        self.respQueue = respQueue
        self.connectionType = connectionType
        self.subsQueue = subsQueue
        self.run = True
    
    def runAction(self):
        while self.run : 
            data = self.subsQueue.get()
            if data['action'] == "subscribe" : 
                self.socket.Subscribe(data['tokens'])
            
            elif data['action'] == "unsubscribe" : 
                self.socket.Unsubscribe(data['tokens'])
            time.sleep(0.1)

    def onOpen(self, *args):
        resp = {"type" : "GENERAL" , "Identifier" : self.identifier, "data" : list(args), "message" : f"Connection Opened - {self.identifier}", "Broker" : self.broker}
        self.respQueue.put(resp)

    def onClose(self, *args):
        resp = {"type" : "CLOSE", "Identifier" : self.identifier, "data" : list(args), "message" : f"Connection Closed - {self.identifier}", "Broker" : self.broker}
        self.respQueue.put(resp)

    def onLtp(self, data):
        resp = {"type" : "DATA", "Identifier" : self.identifier, "data" : data, "message" : "", "Broker" : self.broker}
        self.respQueue.put(resp)

    def onError(self, data):
        resp = {"type" : "ERROR" , "Identifier" : self.identifier, "data" : data, "message" : f"Error - {self.identifier}", "Broker" : self.broker}
        self.respQueue.put(resp)

    def onDepth(self, data):
        pass
    
    def connect(self):
        threading.Thread(target=self.runAction).start()
        self.socket = self._function(accountData=self.accountData,
                       dataType=self.connectionType,
                       onLtp= self.onLtp, 
                       onDepth=self.onDepth,
                       onError=self.onError,
                       onClose=self.onClose,
                       onOpen=self.onOpen)
        self.socket.connect()

class TickHandler():
    tickQueue = queue.Queue()
    def __init__(self, ltpPubSub, respQueue, obj, logger):
        self.respQueue = respQueue
        self.pubsocket = self._zmq_config(**ltpPubSub)
        self.run = True
        self.obj = obj
        self.logger=logger
        threading.Thread(target = self.__checkQueue).start() # Latency to 40ms when data transferred directly from this process.
        threading.Thread(target = self._ticksPush).start() # Latnecy to 15ms when this method is used.
    
    def _zmq_config(self, host, port):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://{host}:{port}".format(host=host, port=str(port)))
        # socket.setsockopt(zmq.TCP_NODELAY, 1)
        return socket

    def __connect(self, host, port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt( zmq.LINGER,100)
        socket.setsockopt( zmq.AFFINITY,1) 
        socket.setsockopt( zmq.RCVTIMEO, 20000) # 20 Seconds timeout
        socket.connect ("tcp://{host}:{port}".format(host = host, port = port))
        return socket
    
    def __sendReq(self, msg):
        socket = self.__connect(**self.obj.zmqConnection)
        socket.send(json.dumps(msg).encode())
        response = json.loads(socket.recv().decode())
        return response

    def __checkQueue(self):
        rSpw = {}
        while self.run: 
            qsize = self.respQueue.qsize()
            for _ in range(qsize):
                message = self.respQueue.get()
                if message['type'] in ["CLOSE"] :
                    self.logger.critical(f"Connection Closed : {message}")
                    # Restarts the Data websocket as soon as it is closed. 
                    threading.Thread(target = lambda : self.__sendReq({"function": "reconnectWs", 
                                                                       "payload" : {"AccountName" : message['Identifier'].split(":")[1]}
                                                                       }), daemon= True).start()
                    
                elif message['type'] in ['ERROR']:
                    # Logs Errors 
                    self.logger.critical(f"Error in Tick Data : {message}") 

                elif message['type'] == "DATA" : 
                    message['data'].update({"time":time.time()})
                    self.tickQueue.put(message['data'])
                
                elif message['type'] == 'GENERAL' : 
                    # Logs general messages
                    self.logger.debug(f"General Update : {message}") 

                time.sleep(0.01)

    def _ticksPush(self):
        while self.run:
            try: 
                qsize = self.tickQueue.qsize()
                data = []
                for _ in range(qsize):
                    message = self.tickQueue.get()
                    data.append('{topic} _&_ {messagedata}'.format(topic = f"tick:{message['token']}", 
                                                                            messagedata = json.dumps(message)))
                
                if data != [] : 
                    self.pubsocket.send_string('{data}'.format(data=data)) #Latency of 12 ms approx
                    
                time.sleep(0.01)
            except: 
                self.logger.exception("Error while pushing ticks")

class ZmqDataWs():
    
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    filehandler = logging.FileHandler('zmqdataws.log')
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    logger.setLevel(logging.DEBUG)

    __runServer = True 
    connections = {}
    wsConnections = {}
    respQueue = Queue()

    def __init__(self, accesstoken, 
                       zmqConnection = {"host" : "127.0.0.1", "port" : "8542"},
                       ltpPubSub = {"host" : "127.0.0.1", "port" : "8543"},
                       orderWs = {"host" : "127.0.0.1", "port" : "8544"},
                       ) :
        
        self.spk = SparkLib(access_token=accesstoken)
        self.__functions = {
            "startWS" : self.connect,
            "reconnectWs" : self.reconnect,
            "stopWs" : self.stop,
            "subscribe" : self.subscribe,
            "unsubscribe" : self.unsubscribe
        }
        self.zmqConnection = zmqConnection
        self.ltpPubSub = ltpPubSub
        self.orderWs = orderWs
        self._zmqConnect()

    def _middlewareLog(func): 
        @wraps(func)
        def wrapper(self, *args, **kwargs): 
            function_name = func.__name__
            data = args
            if data != () : 
                data = args[0]
                try : 
                    st = time.time()
                    result = func(self, *args, **kwargs)
                    ed = time.time()
                    self.logger.debug(f"ZMQ LOGS | from {str(function_name)} : Payload Received: {str(data)}, response : {str(result)}, timeTaken : {str(round(ed-st,2))}")
                    return result
                except Exception as e : 
                    p = traceback.format_exc()
                    self.logger.exception(f"ZMQ LOGS | from {{str(function_name)}} : Traceback : {p}")
                    return {"status" : False, "error" : True, "message" : f"Error : {str(e)}, Traeback : {p}", "data" : []}
            else: 
                return {"status" : False, "error" : True, "message" : f"Payload not available.", "data" : []}
        return wrapper 
    
    def __initiateConn(self, AccountName, connId, connType):
        acDict = self.connections[AccountName]
        self.wsConnections[connId] = {
                                    "AccountName" : AccountName, 
                                    "_limit" : acDict['maxTokens'], 
                                    "_usedLimit" : 0, 
                                    "_queue" : Queue() , 
                                    "tokens" : [], 
                                    "_parseTokens" : [], 
                                    "_unsubTokens" : [],
                                    "_broker" : acDict['Broker'],
                                    "_connType" : connType,
                                    "_seg" : acDict['segments']
                                    }       
        self.wsConnections[connId]["_function"] = brokerWS(accountData=acDict['accountData'],
                                                           respQueue=self.respQueue,
                                                           subsQueue=self.wsConnections[connId]['_queue'],
                                                           connectionType=connType
                                                           )
        self.wsConnections[connId]['_process'] = Process(target= self.wsConnections[connId]['_function'].connect)
        self.wsConnections[connId]['_process'].start()

    def __singleAccount(self, accountName):
        data = self.spk.getOneAccount(accountName)
        if data['status'] and not data['error']:
            return data['data'][0]
        else: 
            return []
        
    @_middlewareLog
    def connect(self, payload):
        if not payload.get("AccountName") : 
            return {"status" : False, "error" : True, "data" : [], "message" : "AccountName missing in payload."} 
        acData = self.__singleAccount(payload['AccountName'])
        if acData == [] : 
            return {"status" : False, "error" : True, "data" : [], "message" : "AccountName not found."} 
        broker = acData['Broker']
        if broker not in BROKERS.keys():
            return {"status" : False, "error" : True, "data" : [], "message" : f"Broker : {broker} not available for ZmqDataWs."}
        elif acData['LastLogin'] != str(datetime.datetime.now().date()) : 
            return {"status" : False, "error" : True, "data" : [], "message" : f"Please generate token for the account : {payload['AccountName']}"}
        
        if payload['AccountName'] not in self.connections.keys() : 
            self.connections[payload['AccountName']] = {
                                                        "accountData" : acData,
                                                        "maxConnections" : BROKERconf[broker]['maxConnection'],
                                                        "curConnections" : 0,
                                                        "maxTokens" : BROKERconf[broker]['tokenLimit'],
                                                        "segments" : BROKERconf[broker]['segments'],
                                                        "connections" : [],
                                                        "Broker" : broker
                                                    }
        connectionType = "ltp" if payload['connectionType'] != "depth" else "depth"
        if connectionType == "depth" : 
            if not BROKERconf[broker]['is_depth'] : 
                return {"status" : False, "error" : True, "message" : f"depth data not supported for Account name : {payload['AccountName']} with broker : {broker}"}

        acDict = self.connections[payload['AccountName']]
        if acDict['curConnections'] + 1 > acDict['maxConnections'] : 
            return {"status" : False, "error" : True, "data":[], 
                    "message" : f"Cannot add new connection on same account name. Current Connection : {acDict['curConnections']}, Max Connections : {acDict['maxConnections']}"}
        
        connIdno = len(acDict['connections']) + 1
        connId = payload['AccountName'] + f"--{str(connIdno)}"
        self.__initiateConn(AccountName=payload['AccountName'], connId= connId, connType=connectionType)
        acDict['connections'].append(connId)
        acDict['curConnections'] += 1
        return {"status" : True, "error" : False, "message" : f"{payload['AccountName']} Connected."}

    def __reconnect(self, accountName, spwId):
        try: 
            con = self.wsConnections[spwId]
            con["_process"].terminate()
            time.sleep(0.2)
            con["_process"].kill()
            con["_process"].close()
            time.sleep(0.2)
            acDict = self.connections[accountName]
            self.wsConnections[spwId]['_queue'] = Queue()
            self.wsConnections[spwId]["_function"] = brokerWS(accountData=acDict['accountData'],
                                                           respQueue=self.respQueue,
                                                           subsQueue=self.wsConnections[spwId]['_queue'],
                                                           connectionType=self.wsConnections[spwId]['_connType']
                                                           )
            self.wsConnections[spwId]['_process'] = Process(target= self.wsConnections[spwId]['_function'].connect)
            time.sleep(0.2)
            self.wsConnections[spwId]['_process'].start()
            time.sleep(0.5)
            # self.wsConnections[spwId]["_process"].start()
            # time.sleep(0.5)
            self.wsConnections[spwId]["_queue"].put({"action" : "subscribe" , 
                                                        "tokens" : self.wsConnections[spwId]['tokens']})
            return True

        except Exception as e :
            self.logger.exception(f"Error with __reconnect, spw ID : {spwId}, Account Name : {accountName} ")
            return False

    @_middlewareLog
    def reconnect(self, payload):
        if not payload.get("AccountName") : 
            return {"status" : False, "error" : True, "data" : [], "message" : "AccountName missing in payload."} 
        accountName = payload['AccountName']
        if accountName not in self.connections.keys():
            return {"status" : False, "error" : True, "data" : [], "message" : f"AccountName : {accountName} not connected, Unable to reconnect."}
        spwnIds = self.connections[accountName]['connections']
        accountRecon = []
        for spwId in spwnIds : 
            accountRecon.append(self.__reconnect(accountName,spwId))
        if all(accountRecon) : 
            return {"status": True, "error" : False, "data" : [],  "message" : "Successfully reconnected."}
        else:
            meta_data = {}
            for i in range(len(spwnIds)):
                status = accountRecon[i]
                spwId = spwnIds[i]
                meta_data[spwId] = {"status" : status}
            return {'status' : True, "error" : False, "data" : [meta_data], "message" : "Issue with reconnect."}

    @_middlewareLog
    def stop(self, payload):
        if not payload.get("AccountName") : 
            return {"status" : False, "error" : True, "data" : [], "message" : "AccountName missing in payload."} 
        accountName = payload['AccountName']
        if accountName not in self.connections.keys():
            return {"status" : False, "error" : True, "data" : [], "message" : f"AccountName : {accountName} not connected, Unable to stop."}
        spwnIds = self.connections[accountName]['connections']
        for spwId in spwnIds : 
            con = self.wsConnections[spwId]
            con["_process"].terminate()
            time.sleep(0.2)
            con["_process"].kill()
            con["_process"].close()
            time.sleep(0.2)
            del self.wsConnections[spwId]
        del self.connections[accountName]
        return {"status" : True, "error" : False, "data" : [], "message" : f"Connections closed with account name {accountName}"}

    @_middlewareLog
    def subscribe(self, payload):
        try: 
            if not payload.get("AccountName") or not payload.get("tokens"): 
                return {"status" : False, "error" : True, "data" : [], "message" : "Invalid Payload."}
            accountName = payload['AccountName']
            spwIds = self.connections[accountName]['connections']
            conntypeData = {k : self.wsConnections[k]['_connType'] for k in spwIds}
            conntypes = ['ltp', 'depth']
            
            conntypeData = {k : [] for k in conntypes}
            [conntypeData[self.wsConnections[spwid]['_connType']].append(spwid) for spwid in spwIds]
            subs = []
            for ctyp in conntypes : 
                mapToks = []
                for tk in payload['tokens'] : 
                    for spwId in conntypeData[ctyp]:
                        conn = self.wsConnections[spwId]
                        if conn['_usedLimit'] < conn['_limit'] and tk not in mapToks and tk.split(":")[0] in conn['_seg'] : #and tk not in conn['tokens']: 
                            conn['_usedLimit'] += 1 
                            conn["_parseTokens"].append(tk)
                            conn["tokens"].append(tk)
                            mapToks.append(tk)

            for spwId in spwIds : 
                conn = self.wsConnections[spwId]
                if conn["_parseTokens"] != [] : 
                    conn["_queue"].put({"action" : "subscribe" , "tokens" : copy.deepcopy(conn["_parseTokens"])})
                    subs.append({"connection" : spwId, "accountName" : accountName, "tokens" : copy.deepcopy(conn["_parseTokens"])})
                conn["_parseTokens"] = [] 
            return {"status" : True, "error" : False, "data" : subs, "message" : "Tokens subscribed"}
        except Exception as e : 
            self.logger.exception("Error with token subscription : ")
            return {"status" : False , "error" : True, "data" : [], "message" : f"Error with token subscription : {str(e)}, Traceback : {traceback.format_exc()}"}
    
    @_middlewareLog
    def unsubscribe(self, payload):
        try: 
            if not payload.get("AccountName") or not payload.get("tokens"): 
                return {"status" : False, "error" : True, "data" : [], "message" : "Invalid Payload."}
            accountName = payload['AccountName']
            spwIds = self.connections[accountName]['connections']
            unsub = []
            for tk in payload['token'] : 
                for spwId in spwIds :
                    conn = self.wsConnections[spwId]
                    if tk in conn['tokens']: 
                        conn['_unsubTokens'].append(tk)
                        conn['_usedLimit'] -= 1
                        conn['tokens'].remove(tk)
            for spwId in spwIds : 
                conn = self.wsConnections[spwId]
                if conn["_unsubTokens"] != [] : 
                    conn["_queue"].put({"action" : "unsubscribe" , "tokens" : copy.deepcopy(conn["_parseTokens"])})
                    unsub.append({"connection" : spwId, "tokens" : copy.deepcopy(self.conn["_parseTokens"])})
                conn["_unsubTokens"] = []
            
            return {"status" : True, "error" : False, "data" : unsub, "message" : "Tokens unsubscribed"}
        except Exception as e : 
            self.logger.exception("Error with token unsubscription : ")
            return {"status" : False , "error" : True, "data" : [], "message" : f"Error with token unsubscription : {str(e)}, Traceback : {traceback.format_exc()}"}

    def terminateConnections(self):
        self.tickHandler.run = False
        for ws in self.wsConnections: 
            con = self.wsConnections[ws]
            con["_process"].terminate()
            time.sleep(0.2)
            con["_process"].kill()
            con["_process"].close()

    def _zmqConnect(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 100)
        socket.bind("tcp://{host}:{port}".format(**self.zmqConnection))
        print("tcp://{host}:{port}".format(**self.zmqConnection))
        self.tickHandler = TickHandler(ltpPubSub=self.ltpPubSub, respQueue=self.respQueue, obj=self,
                                       logger=self.logger)
        try: 
            while self.__runServer:
                try: 
                    if socket.poll(1000):
                        message = socket.recv().decode()
                        self.logger.debug(f"Message Received : {message}")
                        message = json.loads(message)
                        if message.get("function") == None or message.get("payload") == None : 
                            resp = {"status" : False, "error" : True, "message" : "Invalid Function or Payload" , "data" : []}
                            socket.send_json(resp)
                        try: 
                            resp = self.__functions[message['function']](message['payload'])
                        except Exception as e : 
                            resp = {"status" : False, "error" : True, "message" : f"{str(e)} : {traceback.format_exc()}"}
                        socket.send_json(resp)

                except KeyboardInterrupt:
                    self.__runServer = False
                
                except zmq.ZMQError as e:
                    print(f"ZMQError: {e}")
                    self.__runServer = False
                    
                except Exception as e : 
                    self.logger.exception("Error received in ZMQ Connect.")
                    socket.send_json({"status" : False, "error" : True, "data" : [], "message" : f"Error : {str(e)}, Traceback : {traceback.format_exc()}"})
                time.sleep(0.01)
        finally : 
            self.terminateConnections()
            socket.close()
            context.term()


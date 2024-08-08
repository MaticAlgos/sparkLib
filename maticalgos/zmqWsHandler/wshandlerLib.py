import zmq
import time 
import json 
import traceback
import threading

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
    def __init__(self, host = "localhost", port = 8543):
        self.host = host
        self.port = port
        self.tickStream = self.__tickStream
        self.minStream = self.__minStream
        
    def __tickStream(self, message):
        print(message)
        
    def __minStream(self, message):
        print(message)
    
    def __connect(self):
        self.context = zmq.Context().instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.LINGER, 100)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.connect("tcp://{host}:{port}".format(host = self.host, port = self.port))
        try: 
            while self._run : 
                try : 
                    data = self.socket.recv() #flags = zmq.NOBLOCKS
                    
                    # token, message = data.decode().split(" _&_ ")
                    # if token in self.tokens : 
                    #     dt = json.loads(message)
                    #     self.__updateHandler(message = dt, isTick = True if "tick" in token else False)
                    
                    # msgpack
                    # message = msgpack.unpackb(data)
                    # token = message['token']
                    # if token in self.tokens : 
                    #     self.__updateHandler(message = message, isTick = True if message['type'] == "tick" else False)

                    data = ast.literal_eval(data.decode())
                    for d in data : 
                        token, message = d.split(" _&_ ")
                        if token in self.tokens or ("tick" in token and self.__allTicks) or ("min" in token and self.__allMin) : 
                            dt = json.loads(message)
                            self.__updateHandler(message = dt, isTick = True if "tick" in token else False)
                
                except zmq.ZMQError as e : 
                    print(e)
                    self._run = False

                except Exception as e:
                    print(f"Unexpected error: {e}")
                    self._run = False
                    
        finally: pass
    def stop(self):
        self._run = False
        self.socket.close()
        self.context.term()
        
    def connect(self):
        threading.Thread(target = self.__connect).start()
    
    def subscribeAll(self, ticks = True):
        if ticks: 
            self.__allTicks = True
        else: 
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
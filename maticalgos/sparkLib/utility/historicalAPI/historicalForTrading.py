import os
import duckdb
import pandas as pd 
import datetime
import traceback
import copy
import time
from .dependencies import SparkLib
from .historicalData import HistoricalData

class ADD_INSTRUMENTS():
    def __init__(self) -> None:
        self.intrumentsList = []
    
    def __call__(self, name, exchange, getSpot= True, getOptions=True, optExp=[0,1,2,3,4], getFutures=True, futExp=[0,1], *args, **kwargs):
        self.addInstrument(name, exchange, getSpot, getOptions, optExp, getFutures, futExp)

    def addInstrument(self, name, exchange, getSpot= True, getOptions=True, optExp=[0,1,2,3,4], getFutures=True, futExp=[0,1]):
        obj = locals()
        del obj['self']
        self.intrumentsList.append(obj)

class ManageData():
    def __init__(self, duckFunc):
        self.dataStore = []
        self.duckFunc = duckFunc
    
    def addData(self, data, symbol, token, expiry):
        try: 
            [i.update({"datetime":datetime.datetime.strptime(i['datetime'], "%Y-%m-%d %H:%M:%S")}) for i in data]
            expiry = datetime.datetime.strptime(expiry,"%Y-%m-%dT%H:%M:%S") if expiry else datetime.datetime(1900,1,1)
            [i.update({"symbol":symbol, "token" : token, "expiry" :str(expiry)}) for i in data]
            # self.dataStore.extend(data)
            self.duckFunc(data)
        except:
            print(f"data is not available for {symbol}", traceback.format_exc())

    @property
    def DataFrame(self):
        return pd.DataFrame(self.dataStore)

class FetchData():
    fnoExch = {"NSE":"NFO",
               "BSE":"BFO",
               "MCX":"MCX"}
    
    def __init__(self, accountName, spk:SparkLib, hist:HistoricalData, duckFunc):
        self.accountName=accountName
        self.spk = spk
        self.hist = hist
        self.mg = ManageData(duckFunc=duckFunc)
        
    def __fetchData(self, token, startTime, endTime):
        try: 
            accountName=self.accountName
            data = self.hist.getHistorical(accountName=accountName, token=token, startTime=startTime, endTime=endTime)
            if data['status'] and not data['error'] : 
                return data['data']
            else: 
                print(f"ERROR while fetching data for {token}; message : {data}")
        except Exception as e: 
            print(e, traceback.format_exc())

    def fetchData(self, startTime, startTimeFNO,endTime, instruments:ADD_INSTRUMENTS, sleepInterval:int=0.1):
        for ins in instruments.intrumentsList:
            if ins['getSpot'] : 
                spotToken = self.spk.getTokens(ins['name'], ins['exchange'])['data'][0]
                token = ":".join([spotToken['exch_seg'], spotToken['token']])
                data = self.__fetchData(token=token, startTime=startTime,endTime=endTime)
                self.mg.addData(data, symbol=spotToken['name'], token=token, expiry=None)

            if ins['getOptions'] : 
                optExp = self.spk.getExpiry(ins['name'], self.fnoExch[ins['exchange']], instrument="OPT")['data']
                optExp = [optExp[i] for i in ins["optExp"]]
                for exp in optExp: 
                    tokens = self.spk.getTokens(symbol=ins['name'], exchange=self.fnoExch[ins['exchange']],
                                                expiry=exp, instrument="OPT")
                    for tk in tokens['data']: 
                        data = self.__fetchData(token=":".join([tk['exch_seg'], tk['token']]), 
                                              startTime=startTimeFNO,
                                              endTime=endTime)
                        self.mg.addData(data, symbol=tk['symbol'], token=":".join([tk['exch_seg'], tk['token']]), expiry=tk['expiry'])
                        time.sleep(sleepInterval)
            
            if ins['getFutures']:
                futExp = self.spk.getExpiry(ins['name'], self.fnoExch[ins['exchange']], instrument="FUT")['data']
                futExp = [futExp[i] for i in ins["futExp"]]
                for exp in futExp: 
                    tokens = self.spk.getTokens(symbol=ins['name'], exchange=self.fnoExch[ins['exchange']],
                                                expiry=exp, instrument="FUT")
                    for tk in tokens['data']: 
                        data = self.__fetchData(token=":".join([tk['exch_seg'], tk['token']]), 
                                              startTime=startTimeFNO,
                                              endTime=endTime)
                        self.mg.addData(data, symbol=tk['symbol'], token=":".join([tk['exch_seg'], tk['token']]), expiry=tk['expiry'])
        return self.mg.dataStore

class buildHist():
    def __init__(self, filename:str, accessToken:str=None, path:str="./"):
        if not accessToken :
            if os.environ.get("MATICALGOS_AccessToken"):
                accessToken = os.environ["MATICALGOS_AccessToken"] 
            else: 
                raise Exception("Please generate access token.")
        self.spk = SparkLib(access_token=accessToken)
        self.hist = HistoricalData(accessToken)
        self.filename = filename
        self.path = path
        self.__createDuckDB(filename=filename, path=path)

    def __createDuckDB(self, filename, path = "."):
        files = os.listdir(path)
        if filename + ".db" not in files: 
            self.conn = duckdb.connect(r"{path}/{filename}.db".format(path=path,filename=filename))
            self.conn.execute("""
                        CREATE TABLE dbtable (
                            datetime TIMESTAMP,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume INTEGER,
                            oi INTEGER,
                            symbol VARCHAR,
                            token VARCHAR,
                            expiry TIMESTAMP,
                         )
                            """)
            
            self.conn.execute("""
                        CREATE TABLE dataConfig (
                            name VARCHAR,
                            exchange VARCHAR,
                            getSpot VARCHAR,
                            getOptions VARCHAR,
                            optExp VARCHAR,
                            getFutures VARCHAR,
                            futExp VARCHAR,  
                            lastUpdate TIMESTAMP,
                            maxDays INTEGER,
                         )
                            """)
            self.conn.commit()
            self.conn.close()

    def _conn(self):
        return duckdb.connect(r"{path}/{filename}.db".format(path=self.path,filename=self.filename))
    
    def dumpInstruments(self, instruments:ADD_INSTRUMENTS, maxDays:int):
        conn = self._conn()
        an = copy.deepcopy(instruments.intrumentsList)
        [i.update({"optExp":",".join(map(str,i['optExp'])), "futExp" : ",".join(map(str,i['futExp'])), 
                   "lastUpdate":datetime.datetime.now().replace(hour=23,minute=59) - datetime.timedelta(days=1), 
                   "maxDays" : maxDays}) for i in an]
        an = pd.DataFrame(an)
        conn.execute("""INSERT INTO dataConfig SELECT * FROM an""")
        conn.close()

    def chunks(self, data, batch):
        for i in range(0, len(data), batch):
            yield data[i:i + batch]

    def pushDuckDB(self, data, batch=10000):
        try: 
            conn = self._conn()
            # for chunk in self.chunks(data, batch):
                # data = pd.DataFrame(chunk)
                # conn.execute("""INSERT INTO dbtable SELECT * FROM data""")
            data = pd.DataFrame(data)
            if not data.empty: 
                conn.execute("""INSERT INTO dbtable SELECT * FROM data""")
                conn.commit()
                conn.close()
        except Exception as e: 
            print(f"Error {e} while pushing data to duck db ")
            traceback.print_exc()

    def getConf(self):
        conn = self._conn()
        data = conn.execute("""SELECT * FROM dataConfig""").df()
        conn.close()
        return data
    
    def deleteData(self, maxDays):
        conn = self._conn()
        today = datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
        endDt = str(today - datetime.timedelta(days=maxDays))
        conn.execute(f"""DELETE FROM dbtable WHERE datetime < '{endDt}'""")
        conn.commit()
        # conn.execute(f"""DELETE FROM dbtable WHERE expiry < '{today}' and expiry > '1900-01-01 00:00:00'""")
        conn.commit()
        conn.close()

    def createDataBase(self, AccountName:str, instruments:ADD_INSTRUMENTS, maxDays:int=70, maxDaysFNO:int=14,sleepInterval:int=0.2):
        dData = FetchData(accountName=AccountName,spk=self.spk,hist=self.hist, duckFunc=self.pushDuckDB)
        self.dumpInstruments(instruments=instruments, maxDays=maxDays)
        startTime = (datetime.datetime.now() - datetime.timedelta(days=maxDays)).replace(hour=0,minute=0)
        startTimeFNO = (datetime.datetime.now() - datetime.timedelta(days=maxDaysFNO)).replace(hour=0,minute=0)
        endTime = datetime.datetime.now().replace(hour=23,minute=59) - datetime.timedelta(days=1)
        data = dData.fetchData(startTime=startTime,startTimeFNO=startTimeFNO,endTime=endTime,instruments=instruments,sleepInterval=sleepInterval)
        self.pushDuckDB(data)

    def fillData(self, AccountName:str, sleepInterval:int=0.3):
        dData = FetchData(accountName=AccountName,spk=self.spk,hist=self.hist, duckFunc=self.pushDuckDB)
        conf = self.getConf()
        maxDays = int(conf['maxDays'].iloc[0])
        lastUpdate = conf['lastUpdate'].iloc[0]
        self.deleteData(maxDays)
        endTime = datetime.datetime.now()
        ins = ADD_INSTRUMENTS()
        strBool = lambda x: x.lower() == "true"
        for i in range(len(conf)):
            data = dict(conf.iloc[i])
            data['futExp'] = list(map(int,data['futExp'].split(",")))
            data['optExp'] = list(map(int,data['optExp'].split(",")))
            del data['lastUpdate']
            data = {key: strBool(value) if value in ["true", "false"] else value for key, value in data.items()}
            ins(**data)
        data = dData.fetchData(startTime=lastUpdate,startTimeFNO=lastUpdate,endTime=endTime,instruments=ins,sleepInterval=sleepInterval)
        self.pushDuckDB(data)







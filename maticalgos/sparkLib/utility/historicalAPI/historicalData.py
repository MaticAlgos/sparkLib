import datetime
import os 
from .dependencies import SparkLib, FyersHist, AngelHist, iiflHist, UpstoxHist, ShoonyaHist 

BROKERS = {
    "FYERS" : FyersHist,
    "ANGELONE" : AngelHist,
    "SHOONYA" : ShoonyaHist,
    # "KOTAKNEO" : "", #Historical data not available for kotak.
    "UPSTOX" : UpstoxHist,
    "IIFLXTSDATA" : iiflHist
}

class HistoricalData():
    accounts = {}
    startTime = datetime.datetime.now().replace(hour=9,minute=15, second=0, microsecond=0)
    endTime = datetime.datetime.now().replace(hour=15,minute=30, second=0, microsecond=0)
    __today = str(datetime.datetime.now().date())

    def __init__(self, accessToken:str = None):
        if not accessToken :
            if os.environ.get("MATICALGOS_AccessToken"):
                accessToken = os.environ["MATICALGOS_AccessToken"] 
            else: 
                raise Exception("Please generate access token.")
        self.spk = SparkLib(access_token=accessToken)

    def initiateAccount(self, accountName):
        data = self.spk.getOneAccount(accountName=accountName)
        if data['status'] and not data['error']:
            accountData = data['data'][0]
            if accountData['LastLogin'] == self.__today : 
                broker=accountData['Broker']
                sessionData= accountData['Sessionid']
                self.accounts[accountName] = {"sessionid" : sessionData,
                                              "broker" : broker,
                                              "accountName" : accountName,
                                              "accountData" : accountData,
                                              "__function" : BROKERS[broker](accountData)}
            else: 
                raise Exception(f"Generate Code for account Name : {accountName}")
        else: 
            raise Exception(data['message'])

    def _info(self, accountName):
        if accountName not in self.accounts.keys():
            raise Exception(f"Account Name {accountName} not found.")
        return self.accounts[accountName]["__function"].info()

    def getToken(self, token:list, broker):
        broker = "IIFLXTS" if broker == "IIFLXTSDATA" else broker
        tokendata = self.spk.getBrokerTokens(token, broker=broker)
        if tokendata['status'] and not tokendata['error'] : 
            tokenName = tokendata['data']
            if tokenName != [] : 
                return tokenName
            else : 
                raise Exception(f'broker token not found for {token}')
        else: 
            raise Exception(tokendata['message'])

    def getHistorical(self, accountName:str, 
                      token:str,
                      startTime:datetime = startTime, 
                      endTime:datetime=endTime, 
                      interval = "1", reInitialize = False):
        
        if accountName not in self.accounts.keys() or reInitialize :
            self.initiateAccount(accountName)
        accountData = self.accounts[accountName]
        token = self.getToken([token], broker=accountData['broker'])
        if token[0]['histToken'] != None : 
            data = accountData['__function'].getHistorical(token = token[0]['histToken'], 
                                                        startTime = startTime, 
                                                        endTime = endTime, 
                                                        interval = interval)
            return data
        else: 
            raise Exception("Token not found.")
    
    def getQuotes(self, accountName : str, tokens : list, reInitialize = False):
        if accountName not in self.accounts.keys() or reInitialize :
            self.initiateAccount(accountName)
        accountData = self.accounts[accountName]
        tokens = self.getToken(tokens, broker=accountData['broker'])
        # tokens = [i['histToken'] for i in tokens]
        data = accountData['__function'].get_quotes(tokens)
        return data





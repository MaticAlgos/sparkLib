
from maticalgos.sparkLib import SparkLib

spk = SparkLib(apikeys="")

from maticalgos.zmqWsHandler import ZmqDataWs

if __name__ == "__main__":  
    token = spk.generate_token()
    if token['status'] and not token['error'] : 
        accesstoken = token['data'][0]['access_token']
    else: 
        raise(token)
    zm = ZmqDataWs(accesstoken, _priorityTicks = True)
    zm._zmqConnect()

from maticalgos.zmqWsHandler import wsLib, dataStream
from maticalgos.sparkLib import SparkLib

ac = SparkLib(apikeys="Api Keys")
ac.generate_token()

tokens = ac.getTokens("NIFTY", "NFO")
tokens = [f"{i['exch_seg']}:{i['token']}" for i in tokens['data']]
tokens = tokens[:300]

ws = wsLib()

ws.connect("Account Name")
ws.subscribe("Account Name", tokens)

ws.reconnect("Account Name")
ws.stop("Account Name")

############# Stream Data 

def ltp(message):
    print(message)
    
dt = dataStream()
dt.tickStream=ltp 

dt.connect()
#Subscribe to the tokens required in the strategy
# dt.subscribe(tokens)

# dt.stop() 


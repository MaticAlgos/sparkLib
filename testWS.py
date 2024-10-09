from maticalgos.sparkLib import OrderSocket
# from sparkLib import SparkLib
import time

def on_order(message):
    print('test message', message)

def on_error(error):
    print('test_error', error)

def on_close():
    print("### closed ###")

def on_open():
    print(" test on_open called")

order = OrderSocket(access_token='<Your Access Token>',
                    on_order=on_order,
                    on_error=on_error,
                    on_close=on_close,
                    on_connect=on_open,
                    reconnect=True,
                    max_reconnect_attempts=20,
                    run_background=True
                    )
order.connect()
time.sleep(20)
order.close_connection()

###############
from maticalgos.sparkLib.utility import IIFLXTSWS
from maticalgos.sparkLib import SparkLib

spk = SparkLib(apikeys="")
spk.generate_token()

accountData = spk.getOneAccount(accountName= "AccountName")

if accountData["status"] and not accountData['error']:
    accountData = accountData["data"][0]
else: 
    raise Exception(accountData)

def onLTP(message):
    print(message)

def onError(message):
    print(message)

def onClose(message):
    print(message)

xts = IIFLXTSWS(accountData=accountData,
                onLtp=onLTP,
                onError=onError,
                onClose=onClose)

xts.connect()

# Test as per tokens
xts.Subscribe(["NSE:212", "NSE:26009", "NFO:1234", "BSE:2134", "BFO:5678"])

# Close connection 
xts.closeConnection()


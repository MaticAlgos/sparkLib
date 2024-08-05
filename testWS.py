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


from maticalgos.sparkLib import SparkLib
from maticalgos.sparkLib.utility import IIFLXTSWS



spk = SparkLib(apikeys="b1402ac9a97d11f")
spk.generate_token()

data = spk.getOneAccount("IIFLData2")['data'][0]
dataws = IIFLXTSWS(data)
dataws.connect()
dataws.Subscribe(tokens=['NSE:212'])
dataws.closeConnection()

onLTP = None

def parse_requirements(filename):
    with open(filename, 'r') as f:
        return f.read().splitlines()
    

import datetime 
import os 
from ....sparkLib import SparkLib

class AutoLogin():
    def __init__(self, accessToken:str = None):
        if not accessToken :
            if os.environ.get("MATICALGOS_AccessToken"):
                accessToken = os.environ["MATICALGOS_AccessToken"] 
            else: 
                raise Exception("Please generate access token.")
        self.spk = SparkLib(access_token=accessToken)
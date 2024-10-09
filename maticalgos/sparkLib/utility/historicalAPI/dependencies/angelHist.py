import requests
import time
import traceback
import datetime
import re
import uuid
import socket


class angelAPI():
    LTPData_per_sec = 1
    MaxQuoteSymbolLimit = 50
    Historical_per_sec = 3

    BASE_URL = "https://apiconnect.angelbroking.com/" #For V3
    ENDPOINTS = { # For V3
        "getData" : "/rest/secure/angelbroking/historical/v1/getCandleData",
        "getQuotes" : "rest/secure/angelbroking/market/v1/quote"
    }

    HistoricalMapping = {
        "1" : ["ONE_MINUTE", 30], # angel API has a one minute data ,have 30 days limit
        "3" : ["THREE_MINUTE", 60],
        "5" : ["FIVE_MINUTE", 100],
        "10" : ["TEN_MINUTE", 100],
        "15" : ["FIFTEEN_MINUTE", 200],
        "30" : ["THIRTY_MINUTE", 200],
        "60" : ["ONE_HOUR", 400],
        "1D" : ["ONE_DAY", 2000]
    }


    try:
        clientPublicIp = requests.get('https://api.ipify.org').text.strip()
        hostname = socket.gethostname()
        clientLocalIp = socket.gethostbyname(hostname)
    except Exception as e:
        clientPublicIp = "106.193.147.98"
        clientLocalIp = "127.0.0.1"

    clientMacAddress = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
    accept = "application/json"
    userType = "USER"
    sourceID = "WEB"

    def __init__(self,sessionData : dict):
        self.reqsession = requests.Session()
        self.headers = {
            "Content-type": self.accept,
            "X-ClientLocalIP": self.clientLocalIp,
            "X-ClientPublicIP": self.clientPublicIp,
            "X-MACAddress": self.clientMacAddress,
            "Accept": self.accept,
            "X-PrivateKey": sessionData['ApiKey'],
            "X-UserType": self.userType,
            "X-SourceID": self.sourceID
        }
        self.set_accesstoken(sessionData['Sessionid']['jwtToken'])
        self.__supportedFormats = list(self.info()['supported timeformats']["timeValues"].values())

    def set_accesstoken(self, accesstoken):
        self.headers['Authorization'] = f"Bearer {accesstoken}"

    def requestAPI(self, method, url, body=None, data=None, is_header=True, timeout=15):
        try:
            if is_header:
                resp = self.reqsession.request(method, url, json=body, data=data, timeout=timeout,
                                               headers=self.headers)
            else:
                resp = self.reqsession.request(method, url, json=body, data=data, timeout=timeout)
            data = resp.json()
            if data.get("status"):
                return {"status": True, "data": [data], "error": False, "message": "Data Received"}
            else:
                try:
                    return {"status": False, "data": [], "error": True, "message": data['message']}
                except :
                    return {"status": False, "data": [], "error": True, "message": data}

        except requests.exceptions.Timeout:
            return {"status": False, "data": [], "error": True, "message": "Timeout error"}
        except Exception as e:
            try:
                return {"status": False, "data": [], "error": True, "errorMessage": str(e), "message": resp.text}
            except:
                return {"status": False, "data": [], "error": True, "message": str(e)}


    def get_quotes(self, symbols: list,timesleep=0):
        tokenMap = {}
        exchangeTokens = {}
        nonTokens = []

        # Map tokens to exchange:symbol and categorize tokens
        for token in symbols:
            histToken = token.get('histToken')
            if histToken:
                exchange, symbol = histToken.split(", ")
                tokenMap[f'{exchange}:{symbol}'] = token['token']
                exchangeTokens.setdefault(exchange, []).append(symbol)
            else:
                nonTokens.append(token['token'])

        # Check if there are any valid tokens with histToken
        if not exchangeTokens:
            return {"status": False, "data": [], "error": True, "message": "No histToken found for any symbols"}

        # Make API request
        url = f'{self.BASE_URL}{self.ENDPOINTS["getQuotes"]}'
        params = {"mode": 'FULL', "exchangeTokens": exchangeTokens}
        response = self.requestAPI("POST", url, body=params)

        if not response.get('status') or response.get('error'):
            return response

        # Process API response data
        main_data = {}
        fetched_data = response.get('data', [{}])[0].get('data', {}).get('fetched', [])
        if not fetched_data:
            return {"status": False, "data": [], "error": True, "message": "No data received"}

        for mes in fetched_data:
            symbol = f"{mes['exchange']}:{mes['symbolToken']}"
            feedTime = datetime.datetime.strptime(mes['exchFeedTime'], "%d-%b-%Y %H:%M:%S")
            main_data[tokenMap[symbol]] = {
                "timestamp_str": feedTime.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": str(int(feedTime.timestamp())),
                "symbol": tokenMap[symbol],
                "ltp": mes['ltp'],
                "prev_day_close": mes['close'],
                "oi": mes['opnInterest'],
                "prev_day_oi": 0,
                "turnover": mes['tradeVolume'],
                "best_bid_price": mes['depth']['buy'][0]['price'],
                "best_ask_price": mes['depth']['sell'][0]['price'],
                "best_bid_qty": mes['depth']['buy'][0]['quantity'],
                "best_ask_qty": mes['depth']['sell'][0]['quantity'],
                "ttq": mes['tradeVolume'],
                "token": tokenMap[symbol]
            }

        # Include empty data for tokens without histToken
        for token in nonTokens:
            main_data[token] = {}

        return {"status": True, "data": main_data, "error": False, "message": "Data Received"}

    def getHistorical(self, token, startTime: datetime, endTime: datetime, interval: str):
        if interval not in self.HistoricalMapping:
            return {"status": False, "error": True, "message": f"Invalid interval. Please use: {self.info()}", "data": []}

        if not token:
            return {"status": False, "data": [], "error": True, "message": "No histToken found for the symbol"}

        exchange, symboltoken = token.split(", ")
        main_data = []
        endTime = min(endTime, datetime.datetime.now())

        while startTime < endTime:
            ed = min(startTime + datetime.timedelta(days=self.HistoricalMapping[interval][1] - 1), endTime)

            params = {
                "exchange": exchange,
                "symboltoken": symboltoken,
                "interval": interval[0],
                "fromdate": startTime.strftime("%Y-%m-%d %H:%M"),
                "todate": ed.strftime("%Y-%m-%d %H:%M")
            }

            response = self.requestAPI("POST", f"{self.BASE_URL}{self.ENDPOINTS['getData']}", body=params)

            if not response.get('status') or response.get('error'):
                return response

            candle_data = response.get('data', [])
            if candle_data:
                main_data.extend(self.ConvertToSTD(candle_data[0].get('data', [])))
            else:
                return {"status": False, "data": [], "error": True, "message": "Data not available"}

            startTime = ed

        return {"status": True, "data": main_data, "error": False, "message": "Data Received"} if main_data else \
            {"status": False, "data": [], "error": True, "message": "Data not available"}

    def ConvertToSTD(self,data):
        if not data:
            return []
        std_format = [
            {
                'datetime': datetime.datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M:%S"),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': int(candle[5])
            }
            for candle in data
        ]
        return std_format

    def info(self):
        return {"supported timeformats" :
            {"timeValues" :
                {
                    "1 minute" : "1",
                    "3 minute" : "3",
                    "5 minute"  : "5",
                    "10 minute" : "10",
                    "15 minute" : "15",
                    "30 minute" : "30",
                    "60 minute" : "60",
                    "1 Day" : "1D"
                }}}

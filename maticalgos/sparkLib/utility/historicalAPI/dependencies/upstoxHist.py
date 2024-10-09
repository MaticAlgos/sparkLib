import requests
import time
import traceback
import datetime


class UpstoxHist():
    LTPData_per_sec = 1
    MaxQuoteSymbolLimit = 500
    _timeout = 15

    BASE_URL = "https://api.upstox.com/v2/"
    ENDPOINTS = { # For V3
        "getData" : "historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}",
        "getDataIntraday": "historical-candle/intraday/{instrument_key}/{interval}",
        "getQuotes" : "market-quote/quotes"
    }

    HistoricalMapping = {
        "1": ['1minute', 28],    # Upstox has 28 days of 1-minute data
        "30": ['30minute', 364],
        '1D': ['day', 364],
        '1W': ['week', 3640],
        '1M': ['month', 3640]
    }
    headers = {'Accept': 'application/json',
               'Content-Type': 'application/json'}


    def __init__(self,sessionData : dict):
        self.access_token = sessionData['Sessionid']
        self.reqsession = requests.Session()
        self.__supportedFormats = list(self.info()['supported timeformats']["timeValues"].values())
        self.set_accesstoken(self.access_token)

    def set_accesstoken(self,accesstoken):
        self.headers['Authorization'] = f'Bearer {accesstoken}'

    def requestAPI(self, method, url, body=None,params=None, timeout=_timeout, headers = None):
        headers = self.headers if headers == None else headers
        try:
            resp = self.reqsession.request(method, url, headers=headers, json=body, timeout=timeout, params=params)
            data = resp.json()
            if resp.status_code == 200 :
                if data.get("status") == "success" :
                    if isinstance(data['data'], list):
                        return {"status": True, "data": data['data'], "error": False, "message": "Data Received"}
                    else:
                        return {"status": True, "data": [data['data']], "error": False, "message": "Data Received"}
                else:
                    return {'status' : True, "data" : [data], "error": False, "message" : "Data Received"}

            else:
                try :
                    return {"status": False, "data": [], "error": True, "message": data['errors'][0]['message']}
                except :
                    return  {"status": False, "data": [], "error": True, "message": data}

        except requests.exceptions.Timeout:
            return {"status": False, "data": [], "error": True, "message": "Timeout error"}
        except Exception as e:
            try:
                return {"status": False, "data": [], "error": True, "message": resp.text}
            except:
                return {"status": False, "data": [], "error": True, "message":  "Error - Unable to receive data."}

    def get_quotes(self, symbols: list,sleeptime = 0):
        main_data = {}
        tokenMap = {}
        nonTokens = []

        # Map tokens to exchange:symbol and categorize tokens
        for token in symbols:
            histToken = token.get('histToken')
            if histToken:
                tokenMap[histToken] = token['token']
            else:
                nonTokens.append(token['token'])

        if not tokenMap:
            return {
                "status": False,
                "data": [],
                "error": True,
                "message": "No histToken found for any symbols"
            }

        tokenMapKeys = list(tokenMap.keys())
        for i in range(0, len(tokenMapKeys), self.MaxQuoteSymbolLimit):
            instrument_keys = tokenMapKeys[i:i + self.MaxQuoteSymbolLimit]
            query_params = {
                "instrument_key": ",".join(instrument_keys),
            }
            url = f'{self.BASE_URL}{self.ENDPOINTS["getQuotes"]}'
            try:
                response = self.requestAPI("GET", url, params=query_params)
                if not response.get('status') or response.get('error'):
                    return response

                api_data = response.get('data', [])
                if not api_data or not api_data[0]:
                    return {
                        "status": False,
                        "data": [],
                        "error": True,
                        "message": "No data received"
                    }
                api_data = api_data[0]
                for key , message in api_data.items():
                    main_data[tokenMap[message['instrument_token']]] = {
                        "timestamp_str": datetime.datetime.fromtimestamp(int(message['last_trade_time'])//1000).strftime("%Y-%m-%d %H:%M:%S"),
                        "timestamp": str(int(message['last_trade_time'])//1000),
                        "symbol": message['instrument_token'],
                        "ltp": message['last_price'],
                        "prev_day_close": message['ohlc']['close'],
                        "oi": message['oi'],
                        "prev_day_oi": message['oi'],
                        "turnover": message['volume'],
                        "best_bid_price": message['depth']['buy'][0]['price'] if message['depth']['buy'] else 0,
                        "best_ask_price": message['depth']['sell'][0]['price'] if message['depth']['sell'] else 0,
                        "best_bid_qty": message['depth']['buy'][0]['quantity'] if message['depth']['buy'] else 0,
                        "best_ask_qty": message['depth']['sell'][0]['quantity'] if message['depth']['sell'] else 0,
                        "ttq": message['volume'],
                        "token": message['instrument_token']
                    }
            except Exception as e:
                return {
                    "status": False,
                    "data": [],
                    "error": True,
                    "message": f"Error occurred: {str(e)}"
                }
            time.sleep(sleeptime)

        # Fill in empty data for nonTokens
        for token in nonTokens:
            main_data[token] = {}

        return {
            "status": True,
            "data": main_data,
            "error": False,
            "message": "Data Received"
        }

    def getHistorical(self, token, startTime : datetime, endTime : datetime, interval : str): #,sec = False
        if interval not in self.__supportedFormats:
            return {"status" : False, "error" : True, "message" : f"Invalid interval. Please input as per the format: {self.info()}", "data" : []}

        if not token:
            return {"status" : False, "data" : [], "error" : True, "message" : "No histToken found for the symbol"}

        main_data = []
        # check if today's date is in range of start and end date
        if datetime.datetime.now().date() >= startTime.date() and datetime.datetime.now().date() <= endTime.date()  and interval in ['1', '30']:
            Intradays = self.getIntradays(token, self.HistoricalMapping[interval][0])
            if Intradays.get('status') and not Intradays.get('error'):
                main_data.extend(Intradays.get('data', []))
            else:
                return Intradays

        endTime = min(endTime, datetime.datetime.now())
        while startTime < endTime:
            ed = min(startTime + datetime.timedelta(days=self.HistoricalMapping[interval][1]-1), endTime)

            url = f'{self.BASE_URL}{self.ENDPOINTS["getData"]}'.format(
                instrument_key=token,
                interval=self.HistoricalMapping[interval][0],
                to_date=ed.strftime("%Y-%m-%d"),
                from_date=startTime.strftime("%Y-%m-%d")
            )
            response = self.requestAPI("GET", url)
            if not response.get('status') or response.get('error'):
                return response

            candle_data = response.get('data', [])

            if candle_data:
                main_data.extend(self.ConvertToSTD(candle_data[0].get('candles', [])))
            else:
                return {"status": False, "data": [], "error": True, "message": "Data not available"}

            startTime = ed

        return {"status": True, "data": main_data, "error": False, "message": "Data Received"} if main_data else \
                {"status": False, "data": [], "error": True, "message": "Data not available"}

    def ConvertToSTD(self, data):
        if not data:
            return []

        std_format = [
            {
                'datetime': datetime.datetime.fromisoformat(candle[0]).strftime("%Y-%m-%d %H:%M:%S"),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': int(candle[5]),
                'oi': int(candle[6]) if len(candle) == 7 else 0
            }
            for candle in data
        ]

        return std_format

    def getIntradays(self, token, interval : str):
        url = f'{self.BASE_URL}{self.ENDPOINTS["getDataIntraday"]}'.format(
            instrument_key=token,
            interval=interval
        )
        try:
            response = self.requestAPI("GET", url)
            if not response.get('status') or response.get('error'):
                return response

            candle_data = response.get('data', [])

            if candle_data:
                main_data = self.ConvertToSTD(candle_data[0].get('candles', []))
                return {"status": True, "data": main_data, "error": False, "message": "Data Received"}
            else:
                return {"status": False, "data": [], "error": True, "message": "Data not available"}
        except Exception as e:
            return {
                "status": False,
                "data": [],
                "error": True,
                "message": f"Error occurred: {str(e)}"
            }

    def info(self):
        return {"supported timeformats" :
            {"timeValues" :
                {
                    "1 minute" : "1",
                    "30 minute" : "30",
                    "1 Day" : "1D",
                    "1 Week" : "1W",
                    "1 Month" : "1M"
                }}}


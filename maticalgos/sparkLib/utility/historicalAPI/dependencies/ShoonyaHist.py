import requests
import datetime
import json



class ShoonyaHist:
    LTPData_per_sec = 10
    MaxQuoteSymbolLimit = 1
    _timeout = 15

    BASE_URL = "https://api.shoonya.com/NorenWClientTP/"
    ENDPOINTS = { # For V3
        "getData" : "TPSeries",
        "getDataDay" : "EODChartData",
        "getQuotes" : "GetQuotes"
    }

    HistoricalMapping = {
        "1": [1],   # shoonya has 1 minute data ,but no limit on days
        "3": [3],
        "5": [5],
        "10": [10],
        "15": [15],
        "30": [30],
        "60": [60],
        "120": [120],
        "240": [240],
        "1D": ['1D']
    }

    def __init__(self,sessionData : dict):
        self.__susertoken = sessionData['Sessionid']
        self.__username = sessionData['Clientid']
        # self.__accountid = sessionData['username']
        self.reqsession = requests.Session()
        self.__supportedFormats = list(self.info()['supported timeformats']["timeValues"].values())

    def requestAPI(self, method, url, body=None, data=None, timeout=_timeout, output=None, headers=None):
        try:
            if headers:
                resp = self.reqsession.request(method, url, json=body, data=data, timeout=timeout)
            else:
                resp = self.reqsession.request(method, url, json=body, data=data, timeout=timeout, headers=headers)
            data = resp.json()
            if output == 'list' and type(data) == list:
                return {"status": True, "data": data, "error": False, "message": "Data Received"}

            elif data.get("stat") == "Ok":
                return {"status": True, "data": [data], "error": False, "message": "Data Received"}
            else:
                try:
                    return {"status": False, "data": [], "error": True, "message": data['emsg']}
                except :
                    return {"status": False, "data": [], "error": True, "message": data}

        except requests.exceptions.Timeout:
            return {"status": False, "data": [], "error": True, "message": "Timeout error"}
        except Exception as e:
            try:
                return {"status": False, "data": [], "error": True, "message": f"status_code : {resp.status_code}, text : {resp.text}"}
            except:
                return {"status": False, "data": [], "error": True, "message": str(e)}

    def get_quotes(self, symbols: list):
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
            url = f'{self.BASE_URL}{self.ENDPOINTS["getQuotes"]}'
            values              = {}
            values["uid"]       = self.__username
            values["exch"],values["token"] = tokenMapKeys[i].split(", ")
            payload = 'jData=' + json.dumps(values) + f'&jKey={self.__susertoken}'

            try:
                response = self.requestAPI('POST', url, data = payload)
                if not response.get('status') or response.get('error'):
                    return response

                api_data = response.get('data', [])
                if not api_data or not isinstance(api_data[0], dict):
                    return {
                    "status": False,
                    "data": [],
                    "error": True,
                    "message": "No data received"
                    }
                api_data = api_data[0]
                main_data[tokenMap[tokenMapKeys[i]]] = {
                    "timestamp_str": datetime.datetime.fromtimestamp(
                        int(api_data.get('lut', datetime.datetime.now().timestamp()))
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    "timestamp": str(api_data.get('lut', datetime.datetime.now().timestamp())),
                    "symbol": tokenMap[tokenMapKeys[i]],
                    "ltp": api_data.get('lp', 0),
                    "prev_day_close": api_data.get('c', 0),
                    "oi": 0,
                    "prev_day_oi": 0,
                    "turnover": api_data.get('v', 0),
                    "best_bid_price": api_data.get('bp1', 0),
                    "best_ask_price": api_data.get('sp1', 0),
                    "best_bid_qty": api_data.get('bq1', 0),
                    "best_ask_qty": api_data.get('sq1', 0),
                    "ttq": api_data.get('v', 0),
                    "token": tokenMap[tokenMapKeys[i]]
                }
            except Exception as e:
                return {
                    "status": False,
                    "data": [],
                    "error": True,
                    "message": f"Error occurred: {str(e)}"
                }

        # Fill in empty data for nonTokens
        for token in nonTokens:
            main_data[token] = {}

        return {
            "status": True,
            "data": main_data,
            "error": False,
            "message": "Data Received"
        }

    def to_midnight_timestamp(self, dt):
        return int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


    def getHistorical(self, token, startTime : datetime, endTime : datetime, interval : str): #,sec = False
        if interval not in self.__supportedFormats:
            return {"status" : False, "error" : True, "message" : f"Invalid interval. Please input as per the format: {self.info()}", "data" : []}

        if not token:
            return {"status" : False, "data" : [], "error" : True, "message" : "No histToken found for the symbol"}


        startTime = self.to_midnight_timestamp(startTime)
        endTime = min(self.to_midnight_timestamp(endTime), self.to_midnight_timestamp(datetime.datetime.now()))


        if interval == "1D":
            url = f'{self.BASE_URL}{self.ENDPOINTS["getDataDay"]}'
            values              = {}
            values["uid"]       = self.__username
            values["sym"]      = ":".join(token.split(", "))
            values["from"]     = str(startTime)
            values["to"]       = str(endTime)
            headers = {"Content-Type": "application/json; charset=utf-8"}
        else:
            url = f'{self.BASE_URL}{self.ENDPOINTS["getData"]}'
            values              = {'ordersource':'API'}
            values["uid"]       = self.__username
            values["exch"]      = token.split(", ")[0]
            values["token"]     = token.split(", ")[1]
            values["st"] = str(startTime)
            values["en"] = str(endTime)
            values["intrv"] = str(self.HistoricalMapping[interval][0])
            headers = {}

        payload = 'jData=' + json.dumps(values) + f'&jKey={self.__susertoken}'
        response = self.requestAPI('POST', url, data = payload, headers=headers, output='list')

        if not response.get('status') or response.get('error'):
            return response

        candle_data = response.get('data', [])

        if candle_data:
            main_data = self.ConvertToSTD(candle_data)
            main_data = main_data[::-1]
        else:
            return {"status": False, "data": [], "error": True, "message": "Data not available"}

        return {"status": True, "data": main_data, "error": False, "message": "Data Received"} if main_data else \
            {"status": False, "data": [], "error": True, "message": "Data not available"}


    def ConvertToSTD(self, data):
        if not data:
            return []

        std_format = [
            {
                'datetime': datetime.datetime.strptime(candle.get('time'), "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
                'open': candle.get('into'),
                'high': candle.get('inth'),
                'low': candle.get('intl'),
                'close': candle.get('intc'),
                'volume': int(candle.get('intv', 0)),
                'oi': int(candle.get('oi', 0))
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
                    "120 minute" : "120",
                    "240 minute" : "240",
                    "1 Day" : "1D"
                }}}


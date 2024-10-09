import requests
import time
import traceback
import datetime
import json

class IiflHist:
    LTPData_per_sec = 1
    MaxQuoteSymbolLimit = 60

    BASE_URL = "https://ttblaze.iifl.com/apimarketdata/"
    ENDPOINTS = {
        "getData" : "instruments/ohlc",
        "getQuotes" : "instruments/quotes"
    }
    HistoricalMapping = {
        "1S" : [1],   # IIfl has 1 minute data ,but no limit on days
        "1" : [60],
        "2" : [120],
        "3" : [180],
        "5" : [300],
        "10" : [600],
        "15" : [900],
        "30" : [1800],
        "60" : [3600],
        "1D" : [86400]
    }

    def __init__(self,sessionData : dict):
        self.sessionid = sessionData['Sessionid']
        self.reqsession = requests.Session()
        self.header = {
            'Content-Type': 'application/json',
            'authorization': self.sessionid
        }
        self.__supportedFormats = list(self.info()['supported timeformats']["timeValues"].values())

    def requestAPI(self, method, url, body = None, is_header = True, timeout = 15):
        try :
            if not is_header :
                resp = self.reqsession.request(method, url, json = body, timeout = timeout)
            else :
                resp = self.reqsession.request(method, url, json = body, timeout = timeout,
                                               headers = self.header)

            data = resp.json()
            if data.get('type') == 'error' :
                return {"status" : False, "data" : [data], "error" : True, "statuscode" : resp.status_code}

            else :
                return {"status" : True, "data" : [data], "error" : False, "statuscode" : resp.status_code}

        except requests.exceptions.Timeout:
            return {"status" : False, "data" : [], "error" : True,
                    "statuscode" : "", "message" : "Timeout error"}

        except Exception as e :
            return {"status" : False, "data" : [], "error" : True,
                    "statuscode" : resp.status_code, "message" : e}

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
            symbols = []
            tokens = tokenMapKeys[i:i + self.MaxQuoteSymbolLimit]
            for token in tokens:
                symbols.append({
                    "exchangeSegment": token.split(", ")[0],
                    "exchangeInstrumentID": token.split(", ")[1]
                })
            params = {'instruments': symbols, 'xtsMessageCode': 1501, 'publishFormat': "JSON"}
            url = f'{self.BASE_URL}{self.ENDPOINTS["getQuotes"]}'

            try:
                response = self.requestAPI("POST", url, body=params)
                if not response.get('status') or response.get('error'):
                    return response
                api_data = response.get('data', [])
                if not api_data:
                    return {
                        "status": False,
                        "data": [],
                        "error": True,
                        "message": "No data received"
                    }
                api_data = api_data[0]['result']['listQuotes']
                for message in api_data:
                    message =json.loads(message)
                    symbol = f"{message['ExchangeSegment']}, {message['ExchangeInstrumentID']}"
                    main_data[tokenMap[symbol]] = {
                        'timestamp_str': datetime.datetime.fromtimestamp(
                            int(message['LastUpdateTime'])-19800).strftime('%Y-%m-%d %H:%M:%S'),
                        'timestamp': str(int(message['LastUpdateTime'])-19800),
                        'symbol': tokenMap[symbol],
                        'ltp': message['LastTradedPrice'],
                        'prev_day_close': message['Close'],
                        'oi': 0,
                        'prev_day_oi': 0,
                        'turnover': message['TotalTradedQuantity'],
                        'best_bid_price': message['BidInfo']['Price'],
                        'best_ask_price': message['AskInfo']['Price'],
                        'best_bid_qty': message['BidInfo']['Size'],
                        'best_ask_qty': message['AskInfo']['Size'],
                        'ttq': message['TotalTradedQuantity'],
                        'token': tokenMap[symbol]
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

        endTime = min(endTime, datetime.datetime.now())
        main_data = []
        start_date_str = startTime.strftime("%b %d %Y %H%M%S")
        end_date_str = endTime.strftime("%b %d %Y %H%M%S")
        exchangeSegment, exchangeInstrumentID = token.split(", ")
        time_remove = 0 if interval == "1S" else self.HistoricalMapping[interval][0]-1
        try:
            url = f'{self.BASE_URL}{self.ENDPOINTS["getData"]}?exchangeSegment={exchangeSegment}&exchangeInstrumentID={exchangeInstrumentID}&startTime={start_date_str}&endTime={end_date_str}&compressionValue={self.HistoricalMapping[interval][0]}'
            response = self.requestAPI("GET", url)
            if not response.get('status') or response.get('error'):
                return response
            candle_data = response.get('data', [])
            if candle_data and not candle_data[0].get('result', {}).get('dataReponse', '') == '':
                main_data= (self.ConvertToSTD(candle_data[0]['result']['dataReponse'].split(','), time_remove))
            else:
                return {"status": False, "data": [], "error": True, "message": "Data not available"}
        except:
            return {"status": False, "data": [], "error": True, "message": f"Error : {traceback.format_exc()}"}
        
        return {"status": True, "data": main_data, "error": False, "message": "Data Received"} if main_data else \
                {"status": False, "data": [], "error": True, "message": "Data not available"}

    def ConvertToSTD(self, data, request_candle):
        if not data:
            return []
        data = [candle.split('|') for candle in data]
        std_format = [
            {
                'datetime': (datetime.datetime.fromtimestamp(int(candle[0])-request_candle) - datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S"),
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

    def info(self):
        return {"supported timeformats" :
            {"timeValues" :
                {
                    "1 seconds" : "1S",
                    "1 minute" : "1",
                    "2 minute" : "2",
                    "3 minute" : "3",
                    "5 minute"  : "5",
                    "10 minute" : "10",
                    "15 minute" : "15",
                    "30 minute" : "30",
                    "60 minute" : "60",
                    "1 Day" : "1D"
                }}}


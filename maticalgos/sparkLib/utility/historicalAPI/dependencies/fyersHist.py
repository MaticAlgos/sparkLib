import requests
import time
import traceback
import datetime


class FyersHist():
    LTPData_per_sec = 10
    MaxQuoteSymbolLimit = 50

    BASE_URL = "https://api-t1.fyers.in/"
    ENDPOINTS = { # For V3
        "getData" : "data/history?symbol={fyers_token}&resolution={resolution}&date_format={date_format}&range_from={start_date}&range_to={end_date}&cont_flag={cont_flag}&oi_flag={oi_flag}",
        "getQuotes" : "data/quotes?symbols={SYMBOLS}"
    }

    HistoricalMapping = {
        "5S" : ['5S', 30],  # Fyers has 5 seconds data of 30 days limit
        "10S" : ['10S', 30],
        "15S" : ['15S', 30],
        "30S" : ['30S', 30],
        "45S" : ['45S', 30],
        "1" : ['1', 100],
        "2" : ['2', 100],
        "3" : ['3', 100],
        "5" : ['5', 100],
        "10" : ['10', 100],
        "15" : ['15', 100],
        "20" : ['20', 100],
        "30" : ['30', 100],
        "60" : ['60', 100],
        "120" : ['120', 100],
        "240" : ['240', 100],
        "1D" : ['1D', 365]
    }


    def __init__(self,sessionData : dict):
        self.appid = sessionData['ApiKey']
        self.authcode = sessionData['Sessionid']
        self.reqsession = requests.Session()
        self.header = {
                "Accept" : "*/*",
                "Accept-Encoding" : "gzip, deflate, br",
                "Connection" : "keep-alive", 
                "Version" : "2.1"
            }
        self.set_accesstoken()
        self.__supportedFormats = list(self.info()['supported timeformats']["timeValues"].values())

    def set_accesstoken(self):
        self.header['Authorization'] = f"{self.appid}:{self.authcode}"

    def requestAPI(self, method, url, body = None, is_header = True, timeout = 15):
        try :
            if not is_header :
                resp = self.reqsession.request(method, url, json = body, timeout = timeout)
            else :
                resp = self.reqsession.request(method, url, json = body, timeout = timeout,
                                               headers = self.header)

            data = resp.json()

            if data.get('s') == 'error' :
                return {"status" : False, "data" : [data], "error" : True}

            else :
                return {"status" : True, "data" : [data], "error" : False}

        except requests.exceptions.Timeout:
            return {"status" : False, "data" : [], "error" : True, "message" : "Timeout error"}

        except Exception as e :
            return {"status" : False, "data" : [], "error" : True, "message" : e}

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
            j = min(i + self.MaxQuoteSymbolLimit, len(tokenMapKeys))
            symbols_str = ",".join(tokenMapKeys[i:j])
            url = f'{self.BASE_URL}{self.ENDPOINTS["getQuotes"]}'.format(SYMBOLS=symbols_str)

            try:
                response = self.requestAPI("GET", url)
                if not response.get('status') or response.get('error'):
                    return response

                api_data = response.get('data', [])
                if not api_data or not api_data[0].get('d'):
                    return {
                        "status": False,
                        "data": [],
                        "error": True,
                        "message": "No data received"
                    }

                for message in api_data[0]['d']:
                    symbol = message['n']
                    message_values = message['v']
                    main_data[tokenMap[symbol]] = {
                        "timestamp_str": datetime.datetime.fromtimestamp(
                            int(message_values['tt'])
                        ).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S'),
                        "timestamp": str(message_values['tt']),
                        "symbol": tokenMap[symbol],
                        "ltp": message_values['lp'],
                        "prev_day_close": message_values['prev_close_price'],
                        "oi": 0,
                        "prev_day_oi": 0,
                        "turnover": message_values['volume'],
                        "best_bid_price": message_values['bid'],
                        "best_ask_price": message_values['ask'],
                        "best_bid_qty": 0,
                        "best_ask_qty": 0,
                        "ttq": message_values['volume'],
                        "token": tokenMap[symbol]
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

    def getHistorical(self, token, startTime : datetime, endTime : datetime, interval : str):
        if interval not in self.__supportedFormats:
            return {"status" : False, "error" : True, "message" : f"Invalid interval. Please input as per the format: {self.info()}", "data" : []}

        if not token:
            return {"status" : False, "data" : [], "error" : True, "message" : "No histToken found for the symbol"}

        endTime = min(endTime, datetime.datetime.now())
        main_data = []

        while startTime < endTime:
            ed = min(startTime + datetime.timedelta(days=self.HistoricalMapping[interval][1]-1), endTime)

            url = f'{self.BASE_URL}{self.ENDPOINTS["getData"]}'.format(fyers_token=token, resolution=self.HistoricalMapping[interval][0], date_format=0, start_date=int(startTime.timestamp()), end_date=int(ed.timestamp()), cont_flag=1, oi_flag=1)
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
                'datetime': datetime.datetime.fromtimestamp(candle[0]).strftime("%Y-%m-%d %H:%M:%S"),
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
                    "5 seconds" : "5S",
                    "10 seconds" : "10S",
                    "15 seconds" : "15S",
                    "30 seconds" : "30S",
                    "45 seconds" : "45S",
                    "1 minute" : "1",
                    "2 minute" : "2",
                    "3 minute" : "3",
                    "5 minute"  : "5",
                    "10 minute" : "10",
                    "15 minute" : "15",
                    "20 minute" : "20",
                    "30 minute" : "30",
                    "60 minute" : "60",
                    "120 minute" : "120",
                    "240 minute" : "240",
                    "1 Day" : "1D"
                }}}


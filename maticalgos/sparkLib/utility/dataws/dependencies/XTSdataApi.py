# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 12:44:34 2023

@author: Niraj
"""

import requests

ENDPOINTS = {
    'Login':'/auth/login',
    'Quote':'/instruments/quotes',
    'Subscription':'/instruments/subscription', # Same for Unsubscription just PUT instead of POST 
    'indexList' : "/instruments/indexlist?exchangeSegment={exch}" ,
    "optSymbol" :  "/instruments/instrument/optionSymbol?exchangeSegment={exch}&series={series}&symbol={symbol}&expiryDate={exp}&optionType={optype}&strikePrice={strike}",
    "eqSymbol" : "/instruments/instrument/symbol?exchangeSegment={exch}&series={series}&symbol={symbol}",
    "futSymbol" : "/instruments/instrument/futureSymbol?exchangeSegment={exch}&series={series}&symbol={symbol}&expiryDate={exp}"
}

class XTS_MarketData:
    
    def __init__(self, apikey = None, apisecret = None, sessionid = None, URL = None):
        self.BASE_URL = URL
        self.LoginSource = "WebAPI"
        self.apikey = apikey
        self.apisecret = apisecret
        self.reqsession = requests.Session()
        self.header = {'Content-Type': 'application/json'}
        if sessionid != None: 
            self.set_accesstoken(sessionid)
        
    def login(self):
        url = self.BASE_URL + ENDPOINTS["Login"]
        params = {
            "appKey": self.apikey,
            "secretKey": self.apisecret,
            "source": self.LoginSource,
            }
        resp = self.requestAPI("POST", url, body= params)
        if resp['status'] : 
            resp['message'] = "Data Received"
            self.header['Authorization'] = resp['data'][0]['result']['token']
        else : 
            resp['message'] = "Data Received"
        return resp
            
    def set_accesstoken(self, sessionid):
        self.header['Authorization'] = sessionid
        
    def quote(self, instrument):
        params = {'instruments': instrument, 'xtsMessageCode': 1501, 'publishFormat': "JSON"}
        url = self.BASE_URL + ENDPOINTS["Quote"]
        return self.requestAPI("POST", url, body = params)
    
    def subscribe(self, Instruments, xtsMessageCode):
        params = {'instruments': Instruments, 'xtsMessageCode': xtsMessageCode}
        url = self.BASE_URL + ENDPOINTS["Subscription"]
        return self.requestAPI("POST", url, body = params)
    
    def unsubscribe(self, Instruments, xtsMessageCode):
        params = {'instruments': Instruments, 'xtsMessageCode': xtsMessageCode}
        url = self.BASE_URL + ENDPOINTS["Subscription"]
        return self.requestAPI("PUT", url, body = params)

    def getIndexList(self, exchange):
        url = self.BASE_URL + ENDPOINTS["indexList"].format(exch = exchange)
        return self.requestAPI("GET", url)
    
    def optSymbol(self, exchange, series, symbol, exp, opType, strikePrice):
        url = self.BASE_URL + ENDPOINTS['optSymbol'].format(exch = exchange, series = series, symbol = symbol, exp = exp, optype = opType, strike = strikePrice)
        return self.requestAPI("GET", url)
    
    def equitySymbol(self, exchange, series, symbol):
        url = self.BASE_URL + ENDPOINTS['eqSymbol'].format(exch = exchange, series = series, symbol = symbol)
        return self.requestAPI("GET", url)
    
    def futSymbol(self, exchange, series, symbol, exp):
        url = self.BASE_URL + ENDPOINTS['futSymbol'].format(exch = exchange, series = series, symbol = symbol, exp = exp)
        return self.requestAPI("GET", url)
    
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
                    "statuscode" : "resp.status_code", "message" : "Timeout error"}
        
        except Exception as e : 
            return {"status" : False, "data" : [], "error" : True, 
                    "statuscode" : "resp.status_code", "message" : e}
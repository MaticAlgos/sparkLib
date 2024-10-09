# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 13:32:00 2023

@author: Niraj
"""

import requests
import base64
import jwt
import json

# nse_cm - NSE
# bse_cm - BSE
# nse_fo - NFO
# bse_fo - BFO
# cde_fo - CDS

# NRML - Normal
# CNC - Cash and Carry
# MIS - MIS
# CO - Cover Order

# L - Limit
# MKT - Market
# SL - Stop loss limit
# SL-M - Stop loss market

class KotakApi():
    ENDPOINTS = {"AccessToken" : "https://napi.kotaksecurities.com/oauth2/token",
                 "login.validate" : "/login/1.0/login/v2/validate", 
                 "login.generate" : "/login/1.0/login/otp/generate",
                 "login.session" : "/login/1.0/login/v2/validate",
                 "script-master" : "/Files/1.0/masterscrip/v1/file-paths",
                 "order.place" : "/Orders/2.0/quick/order/rule/ms/place?sId={sId}",
                 "order.modify" : "/Orders/2.0/quick/order/vr/modify?sId={sId}",
                 "order.cancel" : "/Orders/2.0/quick/order/cancel?sId={sId}serverId",
                 "order.book" : "/Orders/2.0/quick/user/orders?sId={sId}&hsServerId={hsServerId}",
                 "trade.book" : "/Orders/2.0/quick/user/trades?sId={hsServerId}",
                "position" : "/Orders/2.0/quick/user/positions?sId={hsServerId}",
                 "portfolio" : "/Portfolio/1.0/portfolio/v1/holdings?alt={alt}",
                 "limits" : "/Orders/2.0/quick/user/limits?sId={hsServerId}",
                 "margin" : "/Orders/2.0/quick/user/check-margin?sId={hsServerId}",
                 }
    BASEURL = "https://gw-napi.kotaksecurities.com"
    
    def __init__(self, consumerKey = None, 
                       secretKey = None, 
                       usernameAPI = None, 
                       passwordAPI = None, 
                       phoneNo = None, 
                       password = None, 
                       loginDetails = None):

        self.consumerKey = consumerKey
        self.secretKey = secretKey
        self.passwordAPI = passwordAPI
        self.usernameAPI = usernameAPI
        self.phoneNo = phoneNo
        self.password = password

        self.neofinKey = "neotradeapi"
        self.reqsession = requests.Session()
        self.header = {
                "Accept" : "*/*",
                "Accept-Encoding" : "gzip, deflate, br",
                "Connection" : "keep-alive", 
                "Version" : "2.1",
                "Content-Type" : "application/x-www-form-urlencoded"
            }

        if loginDetails != None:
            self.token = loginDetails['token']
            self.userID = loginDetails['userID']
            self.sid = loginDetails['sid']
            self.hsServerId = loginDetails['hsServerId']
            self.header['Sid'] = self.sid
            self.header['Auth'] = self.token
            self.header['neo-fin-key'] = self.neofinKey
            self.header['Authorization'] = loginDetails['Authorization']

    def get_login_details(self):
        return {"token" : self.header['Auth'], "userID" : self.userID, "sid" : self.sid, "hsServerId" : self.hsServerId, "Authorization" : self.header['Authorization']}

    def accessToken(self, secretKey = None, usernameAPI = None, passwordAPI = None):
        secretKey = secretKey if self.secretKey == None else self.secretKey
        username = usernameAPI if self.usernameAPI == None else self.usernameAPI
        password = passwordAPI if self.passwordAPI == None else self.passwordAPI
        data = ":".join([self.consumerKey, secretKey])
        encodedKeys = base64.b64encode(data.encode()).decode()
        headers = {"Authorization" : f"Basic {encodedKeys}"}
        body = {"grant_type": "client_credentials", "username" : username, "password" : password}
        data = self._requestAPI("POST", self.ENDPOINTS['AccessToken'], body = body, header = headers)
        if data['status'] and not data['error']:
            try: 
                if data['data'][0].get("access_token") != None:
                    self.access_token = data['data'][0]['access_token']
                    self.header['Authorization'] = f"Bearer {self.access_token}"
                    # self.authParams[]
                    data['message'] = "Access Token generated."
                    return data
                else: 
                    data['status'] = False
                    data['error'] = True
                    data['message'] = "Access Token not found"
                    return data
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Access Token not found"
                return data
        else : 
            data['message'] = "Access Token not found"
            return data
        
    def __decodeJWT(self, token):
        return jwt.decode(token, options= {"verify_signature": False})
    
    def loginValidate(self, phoneNo = None, password = None):
        phoneNo = phoneNo if self.phoneNo == None else self.phoneNo
        password = password if self.password == None else self.password
        phoneNo = "+91" + str(phoneNo) if str(phoneNo)[:3] != "+91" else str(phoneNo)
        body = {
                "mobileNumber": phoneNo,
                "password": password,
            }
        url = "".join([self.BASEURL, self.ENDPOINTS['login.validate']])
        data = self._requestAPI("POST", url, body = body)
        if data['status'] and not data['error']:
            try: 
                if data['data'][0].get("data"):
                    respData = data['data'][0]['data']
                    self.token = respData['token']
                    self.userID = self.__decodeJWT(self.token)['sub']
                    self.sid = respData['sid']
                    self.header['Sid'] = self.sid
                    self.header['Auth'] = self.token
                    self.header['neo-fin-key'] = "neotradeapi"
                    data['message'] = "Login validation complete"
                    return data
                else: 
                    data['status'] = False
                    data['error'] = True
                    data['message'] = "Login validation failed"
                    return data
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Login validation failed"
                return data
        else: 
            data['message'] = "Login validation failed"
            return data
    
    def loginGenerate(self):
        body = {
                "userId": self.userID,
                "sendEmail": True,
                "isWhitelisted": True
            }
        url = "".join([self.BASEURL, self.ENDPOINTS['login.generate']])
        data = self._requestAPI("POST", url, body = body)
        try : 
            if data['data'][0].get('error') :
                data['message'] = f"Error {data['data'][0]['error']['message']}, Code {data['data'][0]['error']['code']}"
                
            else: 
                data['message'] = "OTP generated"
                return data
        except: 
            data['message'] = "Otp not generated."
            return data

    def loginSession(self, otp):
        if len(str(otp)) == 4 :
            body = {
                "userId": self.userID,
                "otp": str(otp)
            }
        elif len(str(otp)) == 6 :
            body = {
                "userId": self.userID,
                "mpin": str(otp)
            }
        else:
            return {"status" : False, "error" : True, "message" : "Invalid OTP"}
        url = "".join([self.BASEURL, self.ENDPOINTS['login.session']])
        data = self._requestAPI("POST", url, body = body)
        if data['status'] and not data['error'] : 
            try: 
                if data['data'][0].get('data') : 
                    self.hsServerId = data['data'][0]['data']['hsServerId']
                    self.header['Auth'] = data['data'][0]['data']['token']
                    data['message'] = "Login Successful"
                    return data
                else: 
                    data['status'] = False
                    data['error'] = True
                    data['message'] = "Login Validation failed"
                    return data
                
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Login Validation failed"
                return data
            
        else: 
            data['message'] = "Login Validation failed"
            return data

    def scriptMaster(self):
        url = "".join([self.BASEURL, self.ENDPOINTS['script-master']])
        return self._requestAPI("GET", url)
        
    def placeorder(self, pr, es, pc, qt, tt, pt, ts, am = "NO", tp = 0,dq = 0, mp = 0, pf = "N", rt = "DAY",):
        """
        Parameters
        ----------
        pr : float
            The price to execute the order Ex:2000.00, 250.50, etc.
        es : str
            exchange segment - Eg:nse_cm -> NSE, bse_cm -> BSE, nse_fo -> NFO, bse_fo -> BFO, cde_fo -> CDS, bcs_fo -> BCD, mcx_fo -> MCX .
        pc : str
            Product code (NRML -> Normal, CNC -> Cash and Carry, MIS ->Margin Intraday Squareoff for futures and options, INTRADAY -> INTRADAY, CO->Cover Order, BO -> Bracket Order, PRIME -> Custom product) .
        qt : int
            Quantity to transact (Ex:10, 20, etc) .
        tt : str
            Transaction Type (Buy -> B or Sell -> S) .
        pt : str
            Order Type (L -> Limit, MKT -> Market, SL -> Stop loss limit, SL-M -> Stop loss market) .
        ts : str
            Exchange trading symbol of the instrument Ex:TCS-EQ, ACC-EQ, etc.
        am : str, optional
            After Market Order(Allowed Values -> YES/NO) . The default is "NO".
        dq : int, optional
            Disclosed quantity (Quantity to disclose publicly (for equity trades)) Ex:5, 10 etc . The default is 0.
        mp : float, optional
            Market protection. 0 by default (Ex: 5.00, 8.00 etc) . The default is 0.
        pf : str, optional
            PosSqrFlg (Allowed Values -> Y/N, No Need to change this keep this as N). The default is "N".
        rt : str, optional
            Order Duration (Allowed values - either DAY -> Regular order or IOC -> Immediate or Cancel) . The default is "DAY".
        tp : float
            trigger price
        
        Returns
        -------
        Order ID.

        """
        body = locals()
        del (body["self"])
        body = {"jData" : json.dumps(body)}
        url = "".join([self.BASEURL, self.ENDPOINTS['order.place'].format(sId = self.hsServerId)])
        header = self.header
        header["Content-Type"] = "application/x-www-form-urlencoded"
        data = self._requestAPI("POST", url, data = body,header= header)
        if data['status'] and not data['error'] : 
            try: 
                if data['data'][0].get('nOrdNo') == None :
                    data['status'] = False
                    data['error'] = True
                    data['message'] = data['data'][0]['errMsg']
                else: 
                    data['message'] = "Order Placed Successfully"
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Error in placing order"
        else: 
            try: 
                data['message'] = data['data'][0]['errMsg']
            except : 
                data['message'] = "Unable to Place order"
        return data

    def modifyorder(self, tk, pc, ts, tt, pr, qt, no, es, pt, tp = '0', mp = '0', dd = "NA", dq = '0', vd = "DAY",am = "NO"):
        """
        Parameters
        ----------
        tk : str
            Instrument Token
        pc : str
            Product code (NRML -> Normal, CNC -> Cash and Carry, MIS ->Margin Intraday Squareoff for futures and options, INTRADAY -> INTRADAY, CO->Cover Order, BO -> Bracket Order, PRIME -> Custom product).
        ts : str
            Trading Symbol.
        tt : str
            Transaction Type
        pr : float
            The price to execute the order Ex:2000.00, 250.50, etc.
        qt : int
            Quantity 
        tp : float
            Trigger Price
        no : int
            Order Number
        es : str
            exchange segment - Eg:nse_cm -> NSE, bse_cm -> BSE, nse_fo -> NFO, bse_fo -> BFO, cde_fo -> CDS, bcs_fo -> BCD, mcx_fo -> MCX .
        pt : str
            Order Type (L -> Limit, MKT -> Market, SL -> Stop loss limit, SL-M -> Stop loss market) .
        Returns
        -------
        Order ID

        """
        # fq, am
        qt = str(qt)
        body = locals()
        del (body["self"])
        body = {"jData" : json.dumps(body)}
        url = "".join([self.BASEURL, self.ENDPOINTS['order.modify'].format(sId = self.hsServerId)])  
        header = self.header
        header["Content-Type"] = "application/x-www-form-urlencoded"
        data = self._requestAPI("POST", url, data = body,header= header)
        if data['status'] and not data['error'] : 
            try: 
                if data['data'][0].get('nOrdNo') == None :
                    data['status'] = False
                    data['error'] = True
                    data['message'] = data['data'][0]['message']
                else: 
                    data['message'] = "Order Modified Successfully"
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Error in modifying order"
        else: 
            try: 
                data['message'] = data['data'][0]['errMsg']
            except : 
                data['message'] = "Unable to modify order"
                
        return data
    
    def cancelorder(self, on, am = "NO"):
        body = {"on" : on}
        body = {"jData" : json.dumps(body)}
        url = "".join([self.BASEURL, self.ENDPOINTS['order.cancel'].format(sId = self.hsServerId)])  
        header = self.header
        header["Content-Type"] = "application/x-www-form-urlencoded"
        data = self._requestAPI("POST", url, data = body,header= header)
        if data['status'] and not data['error'] : 
            try: 
                if data['data'][0].get('result') == None :
                    data['status'] = False
                    data['error'] = True
                    data['message'] = data['data'][0]['message']
                else: 
                    data['message'] = "Order Cancelled Successfully"
            except: 
                data['status'] = False
                data['error'] = True
                data['message'] = "Error in cancelling order"
        else: 
            try: 
                data['message'] = data['data'][0]['errMsg']
            except : 
                data['message'] = "Unable to Cancel order"
                
        return data
    
    def orderbook(self):
        url = "".join([self.BASEURL, self.ENDPOINTS['order.book'].format(sId = self.sid, hsServerId = self.hsServerId)])
        data =  self._requestAPI("GET", url)
        if data['data'][0].get("errMsg") == None : 
            data['message'] = "Data Received"
        else: 
            data['message'] = data['data'][0].get("errMsg")
            data['status'] = False
            data['error'] = True
        return data

    def tradebook(self):
        url = "".join([self.BASEURL, self.ENDPOINTS['trade.book'].format(hsServerId = self.hsServerId)])
        data = self._requestAPI("GET", url)
        if data['data'][0].get("errMsg") == None : 
            data['message'] = "Data Received"
        else: 
            data['message'] = data['data'][0].get("errMsg")
            data['status'] = False
            data['error'] = True
        return data

    def position(self):
        url = "".join([self.BASEURL, self.ENDPOINTS['position'].format(hsServerId = self.hsServerId)])
        return self._requestAPI("GET", url)

    def portfolio(self, alt = "false"):
        url = "".join([self.BASEURL, self.ENDPOINTS['portfolio'].format(alt = alt)])
        return self._requestAPI("GET", url)

    def limits(self,segment="ALL", exchange="ALL", product="ALL"):
        payload = {'segment': segment, 'exchange': exchange, 'product': product}
        header_params = {'Authorization':self.header['Authorization'],
                         "Sid": self.sid,
                         "Auth": self.token,
                         "neo-fin-key": self.neofinKey,
                         "accept": "application/json",
                         "Content-Type": "application/x-www-form-urlencoded"}
        url = "".join([self.BASEURL, self.ENDPOINTS['limits'].format(hsServerId = self.hsServerId)])
        return self._requestAPI("POST", url, data = payload, header = header_params)

    def margin(self,exchange_segment, price, order_type, product, quantity, instrument_token, transaction_type,
               trigger_price, broker_name, branch_id, stop_loss_type, stop_loss_value,
               square_off_type, square_off_value, trailing_stop_loss, trailing_sl_value):
        header_params = {'Authorization':self.header['Authorization'],
                         "Sid": self.sid,
                         "Auth": self.token,
                         "neo-fin-key": self.neofinKey,
                         "accept": "application/json",
                         "Content-Type": "application/x-www-form-urlencoded"}
        body_params = {"exSeg": exchange_segment, "prc": price, "prcTp": order_type, "prod": product, "qty": quantity,
                       "tok": instrument_token, "trnsTp": transaction_type, "trgPrc": trigger_price,
                       "brkName": broker_name, "brnchId": branch_id, "slAbsOrTks": stop_loss_type,
                       "slVal": stop_loss_value, "sqrOffAbsOrTks": square_off_type, "sqrOffVal": square_off_value,
                       "trailSL": trailing_stop_loss, "tSLTks": trailing_sl_value}
        url = "".join([self.BASEURL, self.ENDPOINTS['margin'].format(hsServerId = self.hsServerId)])
        return self._requestAPI("POST", url, data = body_params, header = header_params)

    def _requestAPI(self, method, url, body = None, data = None, header = None, timeout = 15):

        header = self.header if header == None else header

        resp = self.reqsession.request(method, url, json = body, data = data,timeout = timeout,
                                       headers = header)
        
        try: 
            data = resp.json()
            if resp.status_code == 200 : 
                return {"error" : False, "status" : True, "data" : [data], }
            
            else: 
                return {"error" : False, "status" : True, "data" : [data] }
        
        except requests.exceptions.Timeout:
            return {"status" : False, "data" : [], "error" : True, "message" : "Timeout error"}
        
        except Exception as e : 
            return {"status" : False, "data" : [], "error" : True, "message" : f"{str(e)} : {resp.text}"}
        
        return resp
        
        
        
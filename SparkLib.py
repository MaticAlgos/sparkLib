# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 02:02:52 2023

@author: Niraj
"""

import requests

class SparkLib():

    BASEURL = "https://apiv.maticalgos.com"

    _routes = {
        "generatetoken" : "/token",
        "profile" : "/profile",
        "order.place" : "/order",
        "order.cancel" : "/order",
        "order.modify" : "/order",
        "order.delete" : "/deleteorder/?strefID={strefID}&reftag={reftag}",
        "master.expiry" : "/expiry?symbol={symbol}&exchange={exchange}&instrument={instrument}",
        "master.token" : "/master-tokens?{body}",
        "account.all" : "/account",
        "account.one" : "/account/{accountName}",
        "account.activate": "/activate-account/{account}/{yn}",
        "authcode.validate" : "/validate_authcode/{broker}",
        "authcode.generate" : "/login/{account}",
        "strategy" : "/strategy",
        "strategy.create" : "/strategy",
        "strategy.modify" : "/strategy/{strategyname}",
        "strategy.delete" : "/strategy/{strategyname}",
        "linkstartegy" : "/linkstrategy",
        "linkstrategyaccount" : "/link-strategy-account?ac={acname}",
        "modifylinkstrategy" : "/linkstrategy?st={st}&ac={ac}",
        "deleteLinkStrategy" : "/linkstrategy?st={st}&ac={ac}",
        "CancalAll_url" : "/CancelAll/{ctype}?stname={stname}&acname={acname}",
        "SquareOff_url" : "/SquareOff/{ctype}?stname={stname}&acname={acname}",
        "orderbook" : "/orderbook/?acname={acname}&stname={stname}",
        "tradebook" : "/tradebook/?startDate={startDate}&endDate={endDate}&stname={stname}&acname={acname}",
        "netposition" : "/netposition/",
        "pushtrades" : "/pushtrades/",
        "overview" : "/overview/",
        "ltp" : "/ltp?Tokens={Tokens}",
        "excutionLogs" : "/ExecutionLogs",
        "intradaypnl" : "/intradaypnl?acname={acname}&stname={stname}",
        "reconnectOrderWS":"/reconnectWS/?acname={acname}",
        "stopOperation":"/stopOperation",
        "squareOffSingle":"/SquareOffSingle?acname={acname}&stname={stname}&token={token}&positionType={positionType}&at_limit={at_limit}",
        "trade.delete" : "/trade/?TDno={TDno}",
        "manualSquareOff" : "/manual-squareoff/?acname={acname}&stname={stname}&token={token}&positionType={positionType}",
        "isHoliday" : "/isHoliday/?exch={exch}",
        "freezeqty" : "/master-freezeqty?symbol={symbol}",
        "contractMaster" : "/contract-master/",
    }
    _timeout = 15

    def __init__(self, userid = None, password = None, apikeys = None, access_token = None):
        self.userid = userid
        self.password = password
        self.apikeys = apikeys
        self.reqsession = requests.Session()
        self.header = {}
        if access_token:
            self.set_AccessToken(access_token)

    def set_AccessToken(self, access_token):
        """
        Set the access token for the session

        Args:
            access_token (str): Access token

        Returns:
            None
        """
        self.header.update({"Authorization" : "Bearer {}".format(access_token)})

    def generate_token(self):
        """
        Generate the token for the session

        Returns
        -------
            dict
        """
        typ = "Account" if not self.apikeys else "Data"
        url = "".join([self.BASEURL, self._routes['generatetoken']])
        data = {"username" : self.userid if typ == "Account" else "Client",
                "password" : self.password if typ == "Account" else "Password"}

        data.update({"client_secret" : self.apikeys}) if typ == "Data" else None

        resp = self._request("POST", url, data = data)

        if not resp.get("access_token") :
            return resp
        else :
            self.set_AccessToken(resp['access_token'])
            return {"stutus" : True, "error" : False, "data" : [resp], "message" : "User Authorized"}

    def profile(self):
        """
        Get the profile of the user

        Returns
        -------
            dict
        """
        url = "".join([self.BASEURL, self._routes['profile']])
        return self._request("GET", url)

    def placeorder(self, strategyName, orderType, productType,  token, qty, transType,limitPrice=0,
                   splitby = 0, triggerPrice = 0, forwardTest = False, operations = {"timeLimit": 0,"shouldExecute": False,"priceBuffer": 0},
                   identifier = None):
        """
        Place the order

        Args:
            strategyName (str): Name of the strategy
            orderType (str): Type of the order (Limit, Market,SL-Limit)
            productType (str): Product type of the order (Intraday, Delivery)
            limitPrice (float): Limit price of the order
            token (str): Token of the order
            qty (int): Quantity of the order
            transType (str): Transaction type of the order (Buy, Sell)
            splitby (int, optional): Split by. Defaults to 0.
            triggerPrice (int, optional): Trigger price. Defaults to 0.
            forwardTest (bool, optional): Forward test. Defaults to False.
            operations (dict, optional): Operations. Defaults to {"timeLimit": 0,"shouldExecute": False,"priceBuffer": 0}.
            identifier ([type], optional): Identifier. Defaults to None.

        Returns
        -------
            dict
        """
        body = { "strategyname": strategyName,
                 "orderType": orderType,
                 "productType": productType,
                 "limitPrice": limitPrice,
                 "token": token,
                 "qty": qty,
                 "transType": transType,
                 "splitby": splitby,
                 "triggerPrice": triggerPrice,
                 "ForwardTest": forwardTest,
                 "operations": operations,
                 "identifier": identifier
                 }
        url = "".join([self.BASEURL, self._routes['order.place']])
        return self._request("POST", url, body = body)

    def modifyorder(self, strategyName, strefID, orderType = None, limitPrice = None, triggerPrice = None, identifier = None):
        """
        Modify the order

        Args:
            strategyName (str): Name of the strategy
            strefID (int): Reference ID of the order
            orderType (str, optional): Type of the order.(Limit, Market,SL-Limit). Defaults to None.
            limitPrice (float, optional): Limit price of the order. Defaults to None.
            triggerPrice (float, optional): Trigger price of the order. Defaults to None.
            identifier (str, optional): Identifier of the order. Defaults to None.
        """

        body = { "strategyname": strategyName,
                 "strefID": strefID,
                 "orderType": orderType,
                 "limitPrice": limitPrice,
                 "triggerPrice": triggerPrice,
                 "identifier": identifier
                 }
        url = "".join([self.BASEURL, self._routes['order.modify']])
        return self._request("PUT", url, body = body)

    def cancelorder(self, strategyName, strefID, identifier = None):
        """
        Cancel the order

        Args:
            strategyName (str): Name of the strategy
            strefID (int): Reference ID of the order
            identifier (str, optional): Identifier of the order. Defaults to None.
        """
        body = { "strategyname": strategyName,
                 "strefID": strefID,
                 "identifier": identifier
                 }
        url = "".join([self.BASEURL, self._routes['order.cancel']])
        return self._request("DELETE", url, body = body)

    def getExpiry(self, symbol, exchange, instrument):
        """
        Get the expiry of the symbol

        Args:
            symbol (str): Symbol of the instrument
            exchange (str): Exchange of the instrument (NFO, BFO)
            instrument (str): Instrument of the symbol (FUT, OPT)
        """
        body = locals()
        url = "".join([self.BASEURL, self._routes['master.expiry'].format(**body)])
        return self._request("GET", url)

    def getTokens(self, symbol, exchange, expiry = None, instrument = None):
        """
        Get the tokens of the symbol

        Args:
            symbol (str): Symbol of the instrument
            exchange (str): Exchange of the instrument (NSE, BSE, NFO, BFO)
            expiry (str, optional): Expiry of the instrument.(format: yyyy-mm-dd). Defaults to None.
            instrument (str, optional): Instrument of the symbol. Defaults to None.
        """
        body = locals()
        del body['self']
        data = {key: value for key, value in body.items() if value is not None}
        st = ""
        for p in data:
            st = st + p + "=" + data[p] + "&"
        st = st[:-1]

        url = "".join([self.BASEURL, self._routes['master.token'].format(body = st)])
        return self._request("GET", url)

    def getAllAccounts(self):
        """
        Get all accounts
        """
        url = "".join([self.BASEURL, self._routes['account.all']])
        return self._request("GET", url)

    def getOneAccount(self, accountName):
        """
        Get one account

        Args:
            AccountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['account.one'].format(accountName = accountName)])
        return self._request("GET", url)

    def generateAuthcode(self, accountName):
        """
        Generate the auth code

        Args:
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['authcode.generate'].format(account = accountName)])
        return self._request("POST", url)

    def validateAuthcode(self, broker, authcode):
        """
        Validate the auth code

        Args:
            broker (str): Name of the broker
            authcode (str): Auth code
        """
        #make upper case of the broker
        url = "".join([self.BASEURL, self._routes['authcode.validate'].format(broker = broker.upper())])
        return self._request("POST", url, data = {"auth_code" : authcode})

    def activateAccount(self, accountName, activate):
        """
        Activate the account

        Args:
            accountName (str): Name of the account
            yn (str): Y or N
        """
        url = "".join([self.BASEURL, self._routes['account.activate'].format(account = accountName, yn = activate.upper())])
        return self._request("POST", url)

    def getStrategy(self):
        """
        Get the strategies
        """
        url = "".join([self.BASEURL, self._routes['strategy']])
        return self._request("GET", url)

    def addStrategy(self, strategyName, Description, StrategyType, Display, ForwardTest):
        """
        Add the strategy

        Args:
            strategyName (str): Name of the strategy
            Description (str): Description of the strategy
            StrategyType (str): Type of the strategy (Intraday, Positional)
            Display (str): Display of the strategy (Public, Private)
            ForwardTest (str): Forward test of the strategy (Y, N)
        """
        body ={'StrategyName': strategyName, 'Description': Description, 'StrategyType': StrategyType, 'Display': Display, 'ForwardTest': ForwardTest}
        url = "".join([self.BASEURL, self._routes['strategy.create']])
        return self._request("POST", url, body = body)

    def modifyStrategy(self, strategyName, Description, StrategyType, Display, ForwardTest):
        """
        Modify the strategy

        Args:
            strategyName (str): Name of the strategy
            Description (str): Description of the strategy
            StrategyType (str): Type of the strategy (Intraday, Positional)
            Display (str): Display of the strategy (Public, Private)
            ForwardTest (str): Forward test of the strategy (Y, N)
        """
        body = locals()
        del body['self']
        url = "".join([self.BASEURL, self._routes['strategy.modify'].format(strategyname = strategyName)])
        return self._request("PUT", url, body = body)

    def deleteStrategy(self, strategyName):
        """
        Delete the strategy

        Args:
            strategyName (str): Name of the strategy
        """
        url = "".join([self.BASEURL, self._routes['strategy.delete'].format(strategyname = strategyName)])
        return self._request("DELETE", url)

    def addlinkStrategy(self, strategyName, accountName, Multiplier, Activate, Capital):
        """
        Add the link strategy

        Args:
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
            Multiplier (int): Multiplier of the strategy
            Activate (str): Activate the strategy
            Capital (int): Capital of the strategy
        """
        if Activate == 'N':
            activate = 0
        elif Activate == 'Y':
            activate = 1
        else:
            raise ValueError("Activate should be either Y or N")
        body ={
            'StrategyName': strategyName,
            'AccountName': accountName,
            'Multiplier': Multiplier,
            'Activate': activate,
            'Capital': Capital
        }

        url = "".join([self.BASEURL, self._routes['linkstartegy']])
        return self._request("POST", url, body = body)

    def modifylinkStrategy(self, strategyName, accountName, Multiplier, Activate, Capital):
        """
        Modify the link strategy

        Args:
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
            Multiplier (int): Multiplier of the strategy
            Activate (str): Activate the strategy
            Capital (int): Capital of the strategy
        """
        activate = 0 if Activate == 'N' else 1
        body = {'Multiplier': Multiplier, 'Activate': activate, 'Capital': Capital}
        #   'https://apiv.maticalgos.com/linkstrategy?st=as&ac=as' \
        url = "".join([self.BASEURL, self._routes['modifylinkstrategy']]).format(st = strategyName, ac = accountName)
        return self._request("PUT", url, body = body)

    def getlinkStrategy(self):
        """
        Get the link strategy
        """
        url = "".join([self.BASEURL, self._routes['linkstartegy']])
        return self._request("GET", url)

    def deletelinkStrategy(self, strategyName, accountName):
        """
        Delete the link strategy

        Args:
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['deleteLinkStrategy']]).format(st = strategyName, ac = accountName)
        return self._request("DELETE", url)

    def getlinkStrategyAccount(self, accountName):
        """
        Get the linked Strategy Per Account

        Args:
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['linkstrategyaccount']]).format(acname = accountName)
        return self._request("GET", url)

    def CancalAll(self, ctype, accountName ,strategyName = 'undefined'):
        """
        Cancel all the orders

        Args:
            ctype (str): Cancel type (account, strategy)
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['CancalAll_url'].format(ctype = ctype, stname = strategyName, acname = accountName)])
        return self._request("GET", url)

    def SquareOff(self, ctype,accountName,strategyName='undefined'):
        """
        Square off the orders

        Args:
            ctype (str): Type of the order (account, strategy)
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['SquareOff_url'].format(ctype = ctype, stname = strategyName, acname = accountName)])
        return self._request("GET", url)

    def orderbook(self,accountName, strategyName, withorders=False,strefid = None, reftag = None):
        """
        Get the order book

        Args:
            accountName (str): Name of the account
            strategyName (str): Name of the strategy
            strefid (str): Reference ID of the order
            reftag (str): Reference tag of the order
            withorders (str): With orders
        """
        url = "".join([self.BASEURL, self._routes['orderbook'].format(acname = accountName, stname = strategyName)])
        if strefid : url = url + "&strefid=" + strefid
        if reftag : url = url + "&reftag=" + reftag
        if withorders : url = url + "&withorders=" + withorders
        return self._request("GET", url)

    def tradebook(self,startDate,endDate,strategyName,accountName):
        """
        Get the trade book

        Args:
            startDate (str): Start date
            endDate (str): End date
            strategyName (str): Name of the strategy
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['tradebook'].format(startDate = startDate, endDate = endDate, stname = strategyName, acname = accountName)])
        return self._request("GET", url)

    def netposition(self,accountName,strategyName):
        """
        Get the net position

        Args:
            accountName (str): Name of the account
            strategyName (str): Name of the strategy
        """
        url = "".join([self.BASEURL, self._routes['netposition']])
        return self._request("GET", url, body = {"acname" : accountName, "stname" : strategyName})

    def pushtrades(self):
        """
        Push the trades

        Args:
            allusers (str): All users
        """
        url = "".join([self.BASEURL, self._routes['pushtrades']])
        return self._request("GET", url)

    def overview(self,accountName):
        """
        Get the overview

        Args:
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['overview']])
        return self._request("GET", url, body = {"acname" : accountName})

    def ltp(self, Tokens):
        """
        Get the LTP

        Args:
            Tokens (str): Tokens
        """
        url = "".join([self.BASEURL, self._routes['ltp']]).format(Tokens = Tokens)
        return self._request("GET", url)

    def excutionLogs(self):
        """
        Get the excution logs
        """
        url = "".join([self.BASEURL, self._routes['excutionLogs']])
        return self._request("GET", url)

    def intradaypnl(self,accountName, strategyName):
        """
        Get the intraday pnl

        Args:
            accountName (str): Name of the account
            strategyName (str): Name of the strategy
        """
        url = "".join([self.BASEURL, self._routes['intradaypnl']]).format(acname = accountName, stname = strategyName)
        return self._request("GET", url)

    def reconnect_orderWS(self,accountName):
        """
        Reconnect the order WS

        Args:
            accountName (str): Name of the account
        """
        url = "".join([self.BASEURL, self._routes['reconnectOrderWS']]).format(acname = accountName)
        return self._request("POST", url)

    def stopOperation(self,strategyName,strefID):
        """
        Stop the operation

        Args:
            strategyName (str): Name of the strategy
            strefID (int): Reference ID of the order
        """
        body = {"strategyname": strategyName, "strefID": strefID}
        url = "".join([self.BASEURL, self._routes['stopOperation']])
        return self._request("POST", url, body = body)

    def squareOffSingle(self,accountName,strategyName,token,positionType,at_limit):
        """
        Square off the single order

        Args:
            accountName (str): Name of the account
            strategyName (str): Name of the strategy
            token (str): Token of the Symbol
            positionType (str): Position type of the order
            at_limit (str): At limit [true, false]
        """
        at_limit = at_limit if at_limit in ['true','false'] else '--'
        url = "".join([self.BASEURL, self._routes['squareOffSingle']]).format(acname = accountName, stname = strategyName, token = token, positionType = positionType, at_limit = at_limit)
        return self._request("POST", url)

    def manualSquareOff(self,accountName,strategyName,token,positionType,tradedPrice,tradedAt,ordersPlaced,qty):
        """
        Manual square off

        Args:
            accountName (str): Name of the account
            strategyName (str): Name of the strategy
            token (str): Token of the Symbol
            positionType (str): Position type of the order
            tradedPrice (float): Traded price
            tradedAt (float): Traded at
            ordersPlaced (float): Orders placed
            qty (int): Quantity
        """
        body = {"tradedPrice": tradedPrice, "tradedAt": tradedAt, "ordersPlaced": ordersPlaced, "qty": qty}
        url = "".join([self.BASEURL, self._routes['manualSquareOff']]).format(acname = accountName, stname = strategyName, token = token, positionType = positionType)
        return self._request("POST", url, body = body)

    def deleteOrder(self,strefID,reftag):
        """
        Delete the order

        Args:
            strefID (int): Reference ID of the order
            reftag (int): Reference id of the order
        """
        url = "".join([self.BASEURL, self._routes['order.delete']]).format(strefID = strefID, reftag = reftag)
        return self._request("DELETE", url)

    def deleteTrade(self,TDno):
        """
        Delete the trade

        Args:
            TDno (str): Trade number
        """
        url = "".join([self.BASEURL, self._routes['trade.delete']]).format(TDno = TDno)
        return self._request("DELETE", url)

    def isHoliday(self,exch):
        """
        Get Holiday Dates list

        Args:
            exch (str): Exchange name
        """
        url = "".join([self.BASEURL, self._routes['isHoliday']]).format(exch = exch)
        return self._request("GET", url)

    def freezeqty(self,symbol):
        """
        Get the freeze quantity for the symbol

        Args:
            symbol (str): Symbol of the instrument
        """
        url = "".join([self.BASEURL, self._routes['freezeqty']]).format(symbol = symbol)
        return self._request("GET", url)

    def contractMaster(self):
        """
        Get the File containing all the contracts information

        """
        url = "".join([self.BASEURL, self._routes['contractMaster']])
        return self._request("GET", url)



    def _request(self, method, url, body = None, data = None, is_header = True, timeout = _timeout):
        """
        Make the request to the server

        Args:
            method (str): Method of the request
            url (str): URL of the request
            body (dict, optional): Body of the request. Defaults to None.
            data (dict, optional): Data of the request. Defaults to None.
            is_header (bool, optional): Header of the request. Defaults to True.
            timeout (int, optional): Timeout of the request. Defaults to _timeout.

        Returns:
            dict
        """
        try :
            resp = self.reqsession.request(method, url, json = body, data = data, timeout = timeout,
                                           headers = self.header)
            print(method, url, body, data, timeout,self.header)
            print(resp.text)
            data = resp.json()
            return data

        except requests.exceptions.Timeout:
            return {"status" : False, "data" : [], "error" : True, "message" : "Timeout error"}

        except requests.exceptions.ConnectionError:
            return {"status" : False, "data" : [], "error" : True, "message" : "Connection error"}

        except Exception as e :
            return {"status" : False, "data" : [], "error" : True, "message" : e}



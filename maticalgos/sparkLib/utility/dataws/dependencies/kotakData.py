import json
import time
import websocket
from threading import Thread
import ssl
from .kotakDataBase import *

class KotakDataFeed:
    def __init__(self,token,sid, server_id,
                 on_message=None,
                 on_error=None,
                 on_close=None,
                 on_open=None,
                 retry_attempts=5
                 ):
        self.access_token = token
        self.sid = sid
        self.server_id = server_id
        self.onmessage = on_message
        self.onerror = on_error
        self.onclose = on_close
        self.onopen = on_open

        self.ws = None
        self.max_retry_attempts = retry_attempts if retry_attempts > -1 else 5
        self.current_retry_attempt = 0
        self.rec_delay = 5

        self.hsWrapper = HSWrapper()
        self.un_sub_token = False
        self.quotes_arr = []
        self.sub_list = []
        self.un_sub_list = []
        self.un_sub_channel_token = {}
        self.channel_tokens = {}
        self.live_scrip_type = None
        self.quotes_index = None
        self.un_sub_list_count = 0
        self.un_sub_channel = None
        self.token_limit_reached = False

    def on_message(self, ws, inData):
        outData = None
        if isinstance(inData, bytes):
            jsonData = self.hsWrapper.parseData(inData,ws)
            # print("JSON DATA in HSWEBSOCKE ON MESSAGE", jsonData)
            if jsonData:
                outData = json.dumps(jsonData) if isEncyptOut else jsonData
        else:
            outData = inData if not isEncyptIn else json.loads(inData) if isEncyptOut else inData
        if outData:
            message = outData
            if type(message) == str:
                req_type = json.loads(message)[0]["type"]
                if req_type == 'cn':
                    if len(self.quotes_arr) >= 1:
                        self.call_quotes()
                    if len(self.sub_list) >= 1:
                        self.subscribe_scripts(self.channel_tokens)
                if req_type == "unsub":
                    if len(self.un_sub_channel_token) > 0 and self.un_sub_channel:
                        # remove from sub_list and sub_token
                        self.remove_items(self.un_sub_channel_token[self.un_sub_channel])
                        del self.un_sub_channel_token[self.un_sub_channel]
                    if len(self.un_sub_channel_token) == 0:
                        if self.token_limit_reached:
                            self.sub_list = []
                            self.channel_tokens = {}
                            self.un_sub_channel_token = {}
                    if self.on_message:
                        self.on_message("Un-Subscribed Successfully!")
            elif type(message) == list:

                # print("raw message ",message)

                # print("quotes ",self.quotes_arr)
                request_type=message[0].get('request_type')
                if request_type and request_type == "SNAP" and (len(self.quotes_arr) >= 1):
                    out_list, quote_type = self.quote_response_formatter(message)
                    if len(out_list)>0:
                        # print("length greater than 0 ")
                        quote_message = self.response_format(out_list, quote_type=quote_type)
                        if self.onmessage:
                            self.onmessage({"type": "quotes", "data": quote_message})
                        self.quotes_arr = []
                if len(self.sub_list) >= 1 and self.is_message_for_subscription(message):
                    if self.onmessage:
                        self.onmessage({"type": "stock_feed", "data": message})

                # If there is no other tokens in quotes_arr and sub_list. disconnect the socket
                # print("sublist size ",len(self.sub_list))
                if(len(self.sub_list)<=0):
                    self.close_websocket()

    def on_error(self, ws, error):
        if self.onerror:
            self.onerror(error)
        else:
            print(f'Error : {error}')
        self.retry_connect()

    def on_close(self, ws, close_status_code, close_msg):
        if self.onclose:
            self.onclose(close_status_code, close_msg)
        else:
            print(f'Closed : {close_status_code} : {close_msg}')

    def on_open(self, ws):
        if self.onopen:
            self.onopen()
        else:
            print('Opened')

    def __on_open_callback(self, ws):
        # print("Connection established HSWebSocket")
        req_params = {"type": "cn", "Authorization": self.access_token, "Sid": self.sid}
        self.hs_send(json.dumps(req_params))
        self.on_open(ws)
        self.current_retry_attempt = 0

    def retry_connect(self):
        if self.current_retry_attempt < self.max_retry_attempts:
            self.current_retry_attempt += 1
            time.sleep(self.rec_delay)
            print(f"Retrying {self.current_retry_attempt} of {self.max_retry_attempts}")
            self.start_websocket()
        else:
            print('Max retry attempts reached')

    def ws_run_forever(self):
        self.ws.run_forever(ping_interval=0, sslopt={"cert_reqs": ssl.CERT_NONE}, reconnect= 5)
                            #ping_timeout=1)

    def start_websocket(self):
        try:
            url = f"wss://mlhsm.kotaksecurities.com"
            self.ws = websocket.WebSocketApp(url,
                                             on_message=self.on_message,
                                             on_error=self.on_error,
                                             on_close=self.on_close,
                                             on_open=self.__on_open_callback)
            self.ws_thread = Thread(target=self.ws_run_forever)
            self.ws_thread.daemon = False
            self.ws_thread.start()

        except Exception as e:
            print(f"Error starting websocket: {e}")
            self.retry_connect()

    def close_websocket(self):
        if self.ws:
            self.ws.close()

    def hs_send(self, d):
        req_json = json.loads(d)
        req_type = req_json[Keys.get("TYPE")]
        # print("Req Type", req_type)
        req = {}
        if Keys.get("SCRIPS") in req_json:
            scrips = req_json[Keys.get("SCRIPS")]
            # print("scrips ", scrips)
            channelnum = req_json[Keys.get("CHANNEL_NUM")]
            # print("CHANNEL NUM ", channelnum)
        else:
            scrips = None
            channelnum = 1
        # scrips = None
        # channelnum = req_json[Keys.get("CHANNEL_NUM")]
        if req_type == ReqTypeValues.get("CONNECTION"):
            if Keys.get("USER_ID") in req_json:
                user = req_json[Keys.get("USER_ID")]
                req = prepare_connection_request(user)
            elif Keys.get("SESSION_ID") in req_json:
                # print("INSIDE SESSION_ID")
                session_id = req_json[Keys.get("SESSION_ID")]
                req = prepare_connection_request(session_id)
            elif Keys.get("AUTHORIZATION") in req_json:
                # print("INSIDE AUTHORIZATION")
                jwt = req_json[Keys.get("AUTHORIZATION")]
                redis_key = req_json[Keys.get("SID")]
                if jwt and redis_key:
                    req = prepareConnectionRequest2(jwt, redis_key)
                    # req = {"Authorization": jwt, "Sid": redis_key}
                else:
                    print("Authorization mode is enabled: Authorization or Sid not found !")
            else:
                print("Invalid conn mode !")
        elif req_type == ReqTypeValues.get("SCRIP_SUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("SUBSCRIBE_TYPE"), SCRIP_PREFIX, channelnum)
            # print("*********** SUB SCRIPS req", req)
        elif req_type == ReqTypeValues.get("SCRIP_UNSUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("UNSUBSCRIBE_TYPE"), SCRIP_PREFIX, channelnum)
        elif req_type == ReqTypeValues.get("INDEX_SUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("SUBSCRIBE_TYPE"), INDEX_PREFIX, channelnum)
        elif req_type == ReqTypeValues.get("INDEX_UNSUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("UNSUBSCRIBE_TYPE"), INDEX_PREFIX, channelnum)
        elif req_type == ReqTypeValues.get("DEPTH_SUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("SUBSCRIBE_TYPE"), DEPTH_PREFIX, channelnum)
        elif req_type == ReqTypeValues.get("DEPTH_UNSUBS"):
            req = prepareSubsUnSubsRequest(scrips, BinRespTypes.get("UNSUBSCRIBE_TYPE"), DEPTH_PREFIX, channelnum)
        elif req_type == ReqTypeValues.get("CHANNEL_PAUSE"):
            req = prepareChannelRequest(BinRespTypes.get("CHPAUSE_TYPE"), channelnum)
        elif req_type == ReqTypeValues.get("CHANNEL_RESUME"):
            req = prepareChannelRequest(BinRespTypes.get("CHRESUME_TYPE"), channelnum)
        elif req_type == ReqTypeValues.get("SNAP_MW"):
            req = prepareSnapshotRequest(scrips, BinRespTypes.get("SNAPSHOT"), SCRIP_PREFIX)
        elif req_type == ReqTypeValues.get("SNAP_DP"):
            req = prepareSnapshotRequest(scrips, BinRespTypes.get("SNAPSHOT"), DEPTH_PREFIX)
        elif req_type == ReqTypeValues.get("SNAP_IF"):
            req = prepareSnapshotRequest(scrips, BinRespTypes.get("SNAPSHOT"), INDEX_PREFIX)
        elif req_type == ReqTypeValues.get("OPC_SUBS"):
            req = get_opc_chain_subs_request(req[Keys.get("OPC_KEY")], req[Keys.get("STK_PRC")],
                                             req[Keys.get("HIGH_STK")],
                                             req[Keys.get("LOW_STK")], channelnum)
        elif req_type == ReqTypeValues.get("THROTTLING_INTERVAL"):
            req = prepareThrottlingIntervalRequest(scrips)
        elif req_type == ReqTypeValues.get("LOG"):
            enable_log(req.get('enable'))
        if self.ws and req:
            self.ws.send(req, 0x2)
        else:
            print("Unable to send request !, Reason: Connection faulty or request not valid !")


    def is_message_for_subscription(self,message):
        # print("message ==== ",message)
        is_for_sub = False
        keys_in_sublist = list({outer_key for data_dict in self.sub_list for outer_key in data_dict})
        # print("sublist keys ",keys_in_sublist)
        for item in message:
            if 'tk' in item:
                if item['tk'] in keys_in_sublist:
                    is_for_sub = True

            if is_for_sub:
                break
        return is_for_sub

    def remove_items(self, un_sub_json):
        for unsubscribe_token in un_sub_json:
            token_value = unsubscribe_token[list(unsubscribe_token.keys())[0]]['instrument_token']
            segment_value = unsubscribe_token[list(unsubscribe_token.keys())[0]]['exchange_segment']
            sub_type_value = unsubscribe_token[list(unsubscribe_token.keys())[0]]['subscription_type']

            # self.sub_list = [token for token in self.sub_list if str(list(token.keys())[0]) != str(token_value)]
            self.sub_list = [token for token in self.sub_list if token != unsubscribe_token]

            for channel_token_list in self.channel_tokens.values():
                for channel_token_dict in channel_token_list:
                    for channel_token_key, channel_token_value in channel_token_dict.items():

                        if token_value == channel_token_value['instrument_token'] and segment_value == \
                                channel_token_value['exchange_segment'] and sub_type_value == \
                                channel_token_value['subscription_type']:
                            channel_token_list.remove(channel_token_dict)
                            break

        return

    def input_validation(self, instrument_tokens):
        valid_params = ["instrument_token", "exchange_segment"]
        ret_obj = True
        if len(instrument_tokens) > 0:
            for item in instrument_tokens:
                if ret_obj:
                    keys_lst = list(item.keys())
                    for key in valid_params:
                        if key in keys_lst:
                            pass
                        else:
                            ret_obj = False
                            break
                else:
                    break
        else:
            ret_obj = False
        return ret_obj

    def get_formatted_data(self, instrument_tokens):
        scrips = ""
        quote_type = ""
        for item in instrument_tokens:
            for k, v in item.items():
                if type(v) == dict and "exchange_segment" in v.keys() and "instrument_token" in v.keys():
                    if scrips != "":
                        scrips += '&'
                    scrips += v["exchange_segment"] + "|" + str(v["instrument_token"])
                if k == "quote_type":
                    quote_type = v
        return scrips, quote_type

    def format_tokens_live(self, instrument_tokens):
        scrips = ""
        if type(instrument_tokens) == dict and "exchange_segment" in instrument_tokens.keys() and \
                "instrument_token" in instrument_tokens.keys():
            if scrips != "":
                scrips += '&'
            scrips += instrument_tokens["exchange_segment"] + "|" + str(instrument_tokens["instrument_token"])
        return scrips

    def format_un_sub_list(self, instrument_tokens):
        scrips = ""
        for instrument_token in instrument_tokens:
            if type(instrument_token) == dict and "exchange_segment" in instrument_token.keys() and \
                    "instrument_token" in instrument_token.keys():
                if scrips != "":
                    scrips += '&'
                scrips += instrument_token["exchange_segment"] + "|" + str(instrument_token["instrument_token"])
        return scrips

    def call_quotes(self):
        scrips, quote_type = self.get_formatted_data(self.quotes_arr)
        scrip_type = ReqTypeValues.get("SNAP_MW")
        if self.quotes_index:
            scrip_type = ReqTypeValues.get("SNAP_IF")
        else:
            if quote_type:
                if quote_type.strip().lower() == 'market_depth':
                    scrip_type = ReqTypeValues.get("SNAP_DP")

        req_params = json.dumps({"type": scrip_type, "scrips": scrips, "channelnum": QuotesChannel})
        self.hs_send(req_params)

    def quote_type_validation(self, quote_type):
        Q_type = True
        if quote_type:
            if str(quote_type).strip().lower() not in ['market_depth', 'ohlc', 'ltp', '52w', 'circuit_limits',
                                                       'scrip_details']:
                Q_type = False
        return Q_type

    def get_quotes(self, instrument_tokens, quote_type=None, isIndex=None):
        if self.quote_type_validation(quote_type):
            self.quotes_index = isIndex
            # self.quotes_api_callback = callback
            if self.input_validation(instrument_tokens):
                for item in instrument_tokens:
                    key = item['instrument_token']
                    value = {'instrument_token': item['instrument_token'],
                             'exchange_segment': item['exchange_segment']}
                    if key not in [list(x.keys())[0] for x in self.quotes_arr]:
                        self.quotes_arr.append({key: value, "quote_type": quote_type})
                    else:
                        index = [list(x.keys())[0] for x in self.quotes_arr].index(key)
                        self.quotes_arr[index][key].update(value)

                self.call_quotes()

            else:
                return Exception("Invalid Inputs")
        else:
            try:
                raise ValueError(json.dumps({"Error": "Quote Type which is given is not matching",
                                             "Expected Values for quote_type": ['market_depth', 'ohlc', 'ltp',
                                                                                '52w',
                                                                                'circuit_limits',
                                                                                'scrip_details']}))
            except ValueError as e:
                print(str(e))

    def subscribe_scripts(self, channel_tokens):
        # print("self.channel_tokens.items()", self.channel_tokens)
        for channel, token_list in channel_tokens.items():
            for tokens in token_list:
                tokens = list(tokens.values())
                scrips = self.format_tokens_live(tokens[0])
                req_params1 = json.dumps(
                    {"type": tokens[0]["subscription_type"], "scrips": scrips, "channelnum": channel})
                self.hs_send(req_params1)

    def prepare_un_sub(self):
        # print("IN Prepare UNSUB")
        for key, value in self.channel_tokens.items():
            # Loop through each item in the value list
            for item in value:
                # Extract the subscription_type from the item
                subscription_type = list(item.values())[0]["subscription_type"]
                subscription_type = subscription_type.replace('s', 'u')
                # Create a new key by appending the subscription_type to the original key
                new_key = f"{key}-{subscription_type}"
                # Create a list to store the items as the value
                new_value = [{list(item.values())[0]["instrument_token"]: list(item.values())[0]}]
                # Add the item to the new_value list
                # Add the new key-value pair to un_sub_channel_token
                if new_key in self.un_sub_channel_token:
                    self.un_sub_channel_token[new_key].extend(new_value)
                else:
                    self.un_sub_channel_token[new_key] = new_value


    def get_live_feed(self, instrument_tokens, isIndex, isDepth):
        if len(self.sub_list) + len(instrument_tokens) > 3000:
            self.token_limit_reached = True
            self.prepare_un_sub()
            self.un_subscription()

        tmp_token_list = []
        subscription_type = ReqTypeValues.get("SCRIP_SUBS")
        if isIndex:
            subscription_type = ReqTypeValues.get("INDEX_SUBS")
        if isDepth:
            subscription_type = ReqTypeValues.get("DEPTH_SUBS")

        if self.input_validation(instrument_tokens):
            for item in instrument_tokens:
                key = item['instrument_token']
                value = {'instrument_token': item['instrument_token'],
                         'exchange_segment': item['exchange_segment'],
                         'subscription_type': subscription_type}
                if 'subscription_type' not in item:
                    item['subscription_type'] = subscription_type
                # if key not in [list(x.keys())[0] for x in self.sub_list]:
                if {key: value} not in self.sub_list:
                    self.sub_list.append({key: value})
                    tmp_token_list.append({key: value})
                # else:
                #     index = [list(x.keys())[0] for x in self.sub_list].index(key)
                #     print("index, key === ", index, key)
                #     print("sub list item === ", self.sub_list[index][key], item)
                #     if self.sub_list[index][key]['exchange_segment'] != item['exchange_segment'] or \
                #             self.sub_list[index][key]['subscription_type'] != item['subscription_type']:
                #         self.sub_list.append({key: value})
                #         tmp_token_list.append({key: value})
                #         print("here 2")
                #
                #     else:
                #         self.sub_list[index][key].update(value)
                #         print("here 3 ")

            channel_tokens = self.channel_segregation(tmp_token_list)
            self.subscribe_scripts(channel_tokens)
        else:
            if self.on_error:
                self.on_error(Exception("Invalid Inputs"))

    def append_ohlc_data(self, new_dict):
        new_dict["ohlc"] = {}
        if 'open' in new_dict.keys():
            new_dict["ohlc"]["open"] = new_dict['open']
            new_dict.pop('open')
        else:
            new_dict["ohlc"]["open"] = None
        if 'high' in new_dict.keys():
            new_dict["ohlc"]["high"] = new_dict['high']
            new_dict.pop('high')
        else:
            new_dict["ohlc"]["high"] = None
        if 'low' in new_dict.keys():
            new_dict["ohlc"]["low"] = new_dict['low']
            new_dict.pop('low')
        else:
            new_dict["ohlc"]["low"] = None
        if 'close' in new_dict.keys():
            new_dict["ohlc"]["close"] = new_dict['close']
            new_dict.pop('close')
        else:
            new_dict["ohlc"]["close"] = None

        return new_dict

    def quote_type_filter(self, new_dict, quote_type):
        if quote_type:
            resp_dict = {'instrument_token': new_dict['instrument_token'],
                         'trading_symbol': new_dict['trading_symbol'],
                         'exchange_segment': new_dict['exchange_segment']}
            if quote_type.strip().lower() == 'ohlc':
                resp_dict['ohlc'] = new_dict['ohlc']
                return resp_dict
            elif quote_type.strip().lower() == 'ltp':
                resp_dict['ltp'] = new_dict['last_traded_price']
                return resp_dict
            elif quote_type.strip().lower() == '52w':
                resp_dict['52week_high'] = new_dict['52week_high']
                resp_dict['52week_low'] = new_dict['52week_low']
                return resp_dict
            elif quote_type.strip().lower() == 'circuit_limits':
                resp_dict['upper_circuit_limit'] = new_dict['upper_circuit_limit']
                resp_dict['lower_circuit_limit'] = new_dict['lower_circuit_limit']
                return resp_dict
            elif quote_type.strip().lower() == 'scrip_details':
                if "open_interest" in new_dict:
                    resp_dict['open_interest'] = new_dict['open_interest']
                resp_dict['last_traded_time'] = new_dict['last_traded_time']
                resp_dict['ltp'] = new_dict['last_traded_price']
                resp_dict['last_traded_quantity'] = new_dict['last_traded_quantity']
                resp_dict['total_buy_quantity'] = new_dict['total_buy_quantity']
                resp_dict['total_sell_quantity'] = new_dict['total_sell_quantity']
                resp_dict['volume'] = new_dict['volume']
                resp_dict['average_price'] = new_dict['average_price']
                resp_dict['volume'] = new_dict['volume']
                resp_dict['change'] = new_dict['change']
                resp_dict['net_change_percentage'] = new_dict['net_change_percentage']
                return resp_dict
            else:
                return new_dict
        else:
            return new_dict

    def depth_resp_mapping(self, response_data):
        final_response = []
        for item in response_data:
            depth_resp = {
                'instrument_token': item['tk'],
                'trading_symbol': item['ts'],
                'exchange_segment': item['e'],
                'depth': {
                    'buy': [
                        {'price': item['bp'], 'quantity': item['bq'], 'orders': item['bno1']},
                        {'price': item['bp1'], 'quantity': item['bq1'], 'orders': item['bno2']},
                        {'price': item['bp2'], 'quantity': item['bq2'], 'orders': item['bno3']},
                        {'price': item['bp3'], 'quantity': item['bq3'], 'orders': item['bno4']},
                        {'price': item['bp4'], 'quantity': item['bq4'], 'orders': item['bno5']},
                    ],
                    'sell': [
                        {'price': item['sp'], 'quantity': item['bs'], 'orders': item['sno1']},
                        {'price': item['sp1'], 'quantity': item['bs1'], 'orders': item['sno2']},
                        {'price': item['sp2'], 'quantity': item['bs2'], 'orders': item['sno3']},
                        {'price': item['sp3'], 'quantity': item['bs3'], 'orders': item['sno4']},
                        {'price': item['sp4'], 'quantity': item['bs4'], 'orders': item['sno5']},
                    ]
                }
            }
            final_response.append(depth_resp)
        return final_response

    def quote_resp_mapper(self, response_data, quote_type=None):
        out_resp = []
        if len(response_data) >= 1:
            for item in response_data:
                if type(item) == dict:
                    new_dict = {stock_key_mapping.get(k, k): v for k, v in item.items()}
                    for key in list(new_dict.keys()):
                        if key not in list(stock_key_mapping.values()):
                            new_dict.pop(key)
                    new_dict = self.append_ohlc_data(new_dict)
                    if quote_type:
                        if quote_type.strip().lower() != 'market_depth':
                            out_resp.append(self.quote_type_filter(new_dict, quote_type))
                    else:
                        out_resp.append(new_dict)
                else:
                    out_resp = response_data
        return out_resp

    def quote_response_formatter(self, message):
        # print("quote response formatter ",message)
        quote_type = ''
        out_list = []
        quotes_arr_list = list(set().union(*(d.keys() for d in self.quotes_arr)))
        # print("quotes arr list ",quotes_arr_list)
        if "quote_type" in quotes_arr_list:
            quotes_arr_list.remove("quote_type")
        for item in message:
            if 'tk' in item:
                if item['tk'] in quotes_arr_list:
                    out_list.append(item)
                    for i in range(len(self.quotes_arr)):
                        if self.quotes_arr[i].get(item['tk']):
                            quote_type = self.quotes_arr[i]["quote_type"]
                            # print("quote type --- ",self.quotes_arr)
                        if self.quotes_arr[i].get(item['tk']):
                            del self.quotes_arr[i]
                            break
        return out_list, quote_type

    def response_format(self, response_data, quote_type):
        # print("response formatter ",response_data)
        # print("quote type ",quote_type)
        out_resp = []
        if self.quotes_index:
            if len(response_data) >= 1:
                for item in response_data:
                    if type(item) == dict:
                        new_dict = {index_key_mapping.get(k, k): v for k, v in item.items()}
                        for key in list(new_dict.keys()):
                            if key not in list(index_key_mapping.values()):
                                new_dict.pop(key)
                        out_resp.append(new_dict)

        else:
            if quote_type:
                if quote_type.strip().lower() == 'market_depth':
                    out_resp = self.depth_resp_mapping(response_data)
                else:
                    out_resp = self.quote_resp_mapper(response_data, quote_type)
            else:
                out_resp = self.quote_resp_mapper(response_data, quote_type)
        return out_resp

    def channel_segregation(self, tmp_token_list):
        # print("****** tmp_token_list", tmp_token_list)
        out_channel_list = {}
        for channel_num in range(2, 17):
            # Check if there is an existing channel array for this channel number
            if channel_num not in self.channel_tokens:
                self.channel_tokens[channel_num] = []
            if channel_num not in out_channel_list:
                out_channel_list[channel_num] = []

            # Check if there is space to add all the input JSON objects to this channel
            if len(self.channel_tokens[channel_num]) + len(tmp_token_list) <= 200:
                # Add all the input JSON objects to this channel
                self.channel_tokens[channel_num].extend(tmp_token_list)
                if out_channel_list[channel_num]:
                    out_channel_list[channel_num].extend(tmp_token_list)
                else:
                    out_channel_list[channel_num] = tmp_token_list
                # Exit the loop since all objects have been added to a channel
                break

            # Otherwise, add as many input JSON objects as possible to this channel
            num_to_add = 200 - len(self.channel_tokens[channel_num])
            self.channel_tokens[channel_num].extend(tmp_token_list[:num_to_add])
            out_channel_list[channel_num].extend(tmp_token_list[:num_to_add])
            # Remove the added objects from the input array
            tmp_token_list = tmp_token_list[num_to_add:]

        return out_channel_list

    def un_subscription(self):
        for channels, token_list in self.un_sub_channel_token.items():
            tokens_list = [list(tokens.values())[0] for tokens in token_list]
            scrips = self.format_un_sub_list(tokens_list)
            self.un_sub_channel = channels
            channel, sub_type = channels.split('-')
            req_params1 = json.dumps(
                {"type": sub_type, "scrips": scrips, "channelnum": channel})
            self.hs_send(req_params1)

    def un_subscribe_list(self, instrument_tokens, isIndex=False, isDepth=False):
        # print("INTO UNSUBSCRIBE", instrument_tokens)
        un_subscription_type = ReqTypeValues.get("SCRIP_UNSUBS")
        subscription_type = ReqTypeValues.get("SCRIP_SUBS")
        if isIndex:
            un_subscription_type = ReqTypeValues.get("INDEX_UNSUBS")
            subscription_type = ReqTypeValues.get("INDEX_SUBS")
        if isDepth:
            un_subscription_type = ReqTypeValues.get("DEPTH_UNSUBS")
            subscription_type = ReqTypeValues.get("DEPTH_SUBS")

        if self.input_validation(instrument_tokens):
            extracted_tokens = [{'instrument_token': item[key]['instrument_token'],
                                 'exchange_segment': item[key]['exchange_segment'],
                                 'subscription_type': item[key]['subscription_type']}
                                for item in self.sub_list for key in item]

            for token in instrument_tokens:
                token["subscription_type"] = subscription_type
                if token in extracted_tokens:
                    for key, value in self.channel_tokens.items():
                        for obj in value:
                            if list(obj.values())[0] == token:
                                in_key = token['instrument_token']
                                value = {'instrument_token': token['instrument_token'],
                                         'exchange_segment': token['exchange_segment'],
                                         'subscription_type': subscription_type}
                                key = str(key) + '-' + un_subscription_type
                                if key not in self.un_sub_channel_token:
                                    self.un_sub_channel_token[key] = []
                                self.un_sub_channel_token[key].append({in_key: value})

                else:
                    print("The Given Token is not in Subscription list")
            if self.ws:
                self.un_subscription()

            else:
                print("Socket Connection has been closed, So! The scripts are already un-subscribed!")


if __name__ == '__main__':
    token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6WyJWaWV3Il0sImV4cCI6MTcxOTUxMzAwMCwianRpIjoiYzBlMTA0ZWMtMjYxMi00YTBjLWFlNWYtNjNiN2QyZWE3NDZjIiwiaWF0IjoxNzE5NDcyOTE4LCJpc3MiOiJsb2dpbi1zZXJ2aWNlIiwic3ViIjoiYThkODA0MTYtOTRjYi00Nzg0LTg0YjctYTRkZDExYTgzMDA2IiwidWNjIjoiWjZZQzAiLCJuYXAiOiIiLCJ5Y2UiOiJlWVxcNiQnOTVcInNcdFxuXHUwMDAwclx1MDAwMFx1MDAxMGIiLCJmZXRjaGNhY2hpbmdydWxlIjowLCJjYXRlZ29yaXNhdGlvbiI6IiJ9.qUoHAL2Duor6DfZ_wa6S_T-95CRWACp0ubUlBegrL16f1eYzPyIjVyuff-5vAmc4RxQyoYlQ-Ie_tK_uXEgD7CDg1klxW32IsYLg-EPtGaxaqWbXgBmu9stoxCAtodgNa9Umm3KwXWRTtsMIlRbXn0QiiATmRsbmrcH_UESNy4CKpVAf3nS85ymPB3R1KPC9dsI5ZJaPSpP_O_t-J1TT0czaGH7rotgQ39cnTnl4YEs07DluCryfra7maDfzRnZ2NxiQhUhEALwZnHuRdj2Ihx6YJXEoOpMsY_y41TvkeB5enm2ApN2XcnGuV6ny5BR_KjemxZvAlfGVBki5bqZDSQ"
    sid = "c8f7cde9-e44f-4df4-9fc9-77997cc9c2e1"
    hsServerId = "server3"

    def custom_on_open():
        inst_tokens = [{"instrument_token": "11536", "exchange_segment": "nse_cm"},
                       {"instrument_token": "1594", "exchange_segment": "nse_cm"},
                       {"instrument_token": "11915", "exchange_segment": "nse_cm"},
                       {"instrument_token": "13245", "exchange_segment": "nse_cm"}]
        soc.get_live_feed(inst_tokens, isIndex=False, isDepth=False)
        print("Custom open handler")

    def custom_on_message(message):
        print(f"Custom message handler: {message}")

    def custom_on_close(close_status_code, close_msg):
        print("Custom close handler : ", close_status_code, close_msg)

    def custom_on_error(error):
        print(f"Custom error handler: {error}")


    soc = KotakDataFeed(token,sid,hsServerId,custom_on_message,custom_on_error,custom_on_close,custom_on_open,retry_attempts=2)
    soc.start_websocket()
    time.sleep(2)
    soc.close_websocket()



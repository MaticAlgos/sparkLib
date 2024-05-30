# SparkLib - MaticAlgos Python Client

SparkLib is a Python client library for interacting with https://spark.maticalgos.com . It provides functionalities to manage accounts, strategies, place orders, and much more.

## Installation

You can install the pre release via pip

```
pip install maticalgos-spark
```

<!-- # Basic Introduction to Spark by MaticAlgos
## Spark Platform: Empowering Multi-Account Trading with Diverse Strategies -->

<!-- At SparkMatic, our main aim is to revolutionize the landscape of algorithmic trading by offering a comprehensive platform that enables traders to execute strategies across multiple accounts seamlessly. With our innovative approach, traders can harness the power of diverse trading strategies, allocate varying amounts of capital, and utilize custom multipliers to optimize their trading performance.

### Key Features:

1. **Multi-Account Trading:** SparkMatic allows traders to manage and execute trades across multiple accounts simultaneously. Whether you're trading in equities,options,futures, forex or camodity  our platform provides the flexibility to execute strategies across various brokerage accounts with ease.

2. **Diverse Strategies:** SparkMatic caters to a wide range of trading strategies. Whether you're a seasoned quantitative analyst or a novice trader, our platform offers the tools and capabilities to implement your preferred strategies effectively.

3. **Custom Capital Deployment:** With SparkMatic, traders have the freedom to allocate capital across different accounts for different strategies based on their risk tolerance and investment objectives. Whether you're looking to pursue aggressive growth opportunities or preserve capital with conservative strategies, our platform empowers you to customize your capital deployment according to your preferences.

4. **Flexible Multipliers:** SparkMatic allows traders to apply custom multipliers to their strategies linked to broker account, enabling precise control over position sizing and risk management. Whether you're scaling your positions for maximum impact or adjusting risk exposure dynamically, our platform provides the flexibility to fine-tune your trading parameters according to market conditions.

5. **Order Slicing:** In addition to multi-account trading, SparkMatic offers advanced order slicing capabilities, allowing traders to split large orders into smaller segments and execute them gradually. By breaking down big orders into manageable chunks, traders can minimize market impact, improve execution quality, and enhance overall trading performance.

With SparkMatic, traders can take their algorithmic trading strategies to the next level, leveraging cutting-edge technology and sophisticated tools to achieve their financial goals with confidence and precision. -->


# Setup Guide

Follow these steps to set up and start trading with Spark:

## Prerequisites for Trading

Before you can begin trading on Spark, ensure you've completed the following steps:

1. **Account Creation**:
   - Sign up for an account on [Spark](https://spark.maticalgos.com/login).

2. **Broker Account Setup**:
   - Have an active broker account with one of the supported brokers.

3. **Connection Establishment**:
   - Connect your broker account to Spark seamlessly.

4. **Strategy Creation**:
   - Develop your trading strategy within the Spark platform.

5. **Strategy Linking**:
   - Associate your created strategy with your broker account on Spark.

6. **Strategy Activation**:
   - Activate your linked strategy within the Spark interface.

7. **Trading Activation**:
   - Enable trading on your broker account within Spark by specifying the Capital Deployed and Multiplier.

8. **Access Token Generation**:
   - Generate an access token to initiate trading activities securely.

9. **Commence Trading**:
   - Once all setup steps are completed, you're ready to commence trading using Spark.



## Control Flow

1. **Login to Your Account**
   - Visit [Spark](https://spark.maticalgos.com/login) and log in using your user ID and password.

2. **Add a Broker Account**
   - Navigate to 'Connections' -> 'ACCOUNTS' -> 'Add Account'.
   - Choose your preferred broker.
   - Provide a unique account name and your client ID.
   - Optionally, add your Telegram bot key and chat ID.
   - Click on 'Add Account'.

3. **Create Strategy**
   - Go to 'Connections' -> 'STRATEGIES' -> 'Add Strategy'.
   - Name your strategy and describe it.
   - Select the strategy type (Intraday or Positional).
   - Choose the display type (Private or Public).
   - Decide if you want to perform a forward test (Yes or No).
   - Click on 'Add Strategy'.

4. **Link Strategy to Connected Broker Accounts**
   - Click on 'Connections' -> 'LINKED STRATEGIES' -> 'Add Linked Strategy'.
   - Select the strategy and the associated account.
   - Specify the multiplier and capital deployed.
   - Click on 'Add'.

5. **Activate Strategy**
   - Visit 'Connections' -> 'LINKED STRATEGIES'.
   - If the strategy status is 'Inactive', click on the 'Activate' button.

6. **Start Trading**
   - Navigate to 'Connections' -> 'ACCOUNTS'.
   - Generate an access token by clicking on 'Generate Token' for the respective account.
   - Click on the 'Trade' button for the respective account.
   - Choose the strategy name and click on 'Activate Now'.

## Getting API Keys
api key is unique for each and every account.
1. Login to your Spark [account](https://spark.maticalgos.com/login).
1. Click on 'Account Name' -> 'Settings'.
1. Copy the 'App Key'. You will need this to generate a session ID.


## Getting an Access Token

1. **Import the Spark library**

```python
from maticalgos.sparkLib import SparkLib, OrderSocket
```

2. **Create a Spark object**
```python
spark = SparkLib(userid = "Your Emailid", 
                password = "Your Password")

# OR

spark = SparkLib(apikeys = "Your API Keys")
```

3. **Generate a access token**
```python
spark.generate_token()
```

Sample response
```
{"access_token":"eyJhbGcUW_FLiIUf9T4j1bFhRg","token_type":"bearer"}
```



## API Methods
1. [Profile](#profile)
2. [Account Info](#account-info)
   1. [All Accounts](#all-accounts)
   2. [One Account](#one-account)
3. [Account Management](#account-management)
   1. [Activate Account](#activate-account)
   2. [Deactivate Account](#deactivate-account)
4. [Strategies Management](#strategies-management)
   1. [Create Strategy](#create-strategy)
   2. [Get Strategies](#get-strategies)
   3. [Modify Strategy](#modify-strategy)
   4. [Delete Strategy](#delete-strategy)
5. [Linked Strategies](#linked-strategies)
   1. [Link Account to Strategy](#link-account-to-strategy)
   2. [Get Linked Strategies](#get-linked-strategies)
   3. [Get Linked Strategies By Account](#get-linked-strategies-by-account)
   4. [Modify Linked Strategy](#modify-linked-strategy)
      1. Modify Linked Strategy
      2. Activate Linked Strategy
      3. Deactivate Linked Strategy
   5. [Delete Linked Strategy](#delete-linked-strategy)

6. [Expiry Dates](#get-expiry-dates)
7. [Tokens](#get-tokens)
8. [Order Management](#order-management)
   1. [LTP](#ltp)
   1. [Place order](#place-an-order)
   2. [Modify order](#modify-order)
   3. [Cancel order](#cancel-an-order)
   4. [Delete Order](#delete-order)
   4. [Square off All Orders](#square-off)
   5. [Cancel All Orders](#cancel-all-orders)
   6. [Stop Operation](#stop-operation)
   7. [Square Off Single](#square-off-single)
   8. [Delete Trade](#delete-order)
   9. [Manual Square Off](#manual-square-off)

10. [Orderbook](#orderbook)
11. [Tradebook](#get-tradebook)
12. [Netposition](#netposition)
13. [Push Trades](#push-trades)
13. [Execution Logs](#get-excution-logs)
14. [Holiday List](#get-holiday-list)
15. [Freeze Quantity](#freeze-quantity)
16. [Reconnect Order Websocket](#reconnect-order-websocket)
17. [Contract Master File](#contract-master-file)

### Profile

This method is used to get the profile of the user.

Code
```python
print(spark.profile()) # get profile
```
Sample response
```
{'status': True, 'error': False, 'message': 'User exists', 'data': [{'ID': 10, 'Name': 'Your Name', 'Email': 'Your Email', 'PhoneNo': 'Your Phone No', 'LastLogin': '2024-04-02', 'CreatedOn': '2024-02-27', 'UCC': 'Your Unique User Code', 'Disabled': 0, 'UserType': 'admin', 'acLimit': 10, 'stLimit': 20}]}
```


## Account INFO
We can get the account information using the following methods
of all accounts and one account.
1. [All Accounts](#all-accounts)
2. [one Account](#one-account)

#### All Accounts
```python
print(spark.getAllAccounts())
```
Sample response

```
{'status': True, 'error': False, 'data': [{'Clientid': 'Your Client ID', 'AccountName': 'Your Account Name', 'LastLogin': '2024-04-03', 'UCC': 'Your Unique User Code', 'Trade': True, 'Broker': 'Your Broker'}], 'message': 'Data Received'}
```

#### One Account
We have to pass the account name to get the account information.
```python
print(spark.getOneAccount(accountName='Your Account Name'))
```
Sample response
```
{'status': True, 'error': False, 'data': [{'Clientid': 'Your Client ID', 'Password': None, 'ApiKey':'Your API Key', 'ApiSecret': 'Your API Secret', 'Totp': 'na', 'AccountName': 'Your Account Name', 'LastLogin': '2024-04-03', 'UCC': 'Your Unique User Code', 'Chatid': 'Your Chat ID', 'BotKeys': 'Your Telegram Bot Key', 'Sessionid': 'Your Session ID', 'Trade': 'Y', 'Broker': 'Your Broker', 'Redirecturl': 'http://spark.maticalgos.com/auth_code/<Your Broker>', 'Pin': 'na'}], 'message': 'Data Received'}
```

## Account Management
For activating and deactivating the Broker account on the Spark platform.We can use the following methods.
1. [Activate Account](#Activate-Account)
2. [Deactivate Account](#deactivate-account)


### Activate account
```python
print(spark.activateAccount(accountName='Your Account Name',activate='Y'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{"Trade": "Y"}], 'message': 'Account Activated.'}
```

### Deactivate Account
```python
print(spark.deactivateAccount(accountName='Your Account Name',activate='N'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{"Trade": "N"}], 'message': 'Account Deactivated.'}
```

| Input Variables | Possible Values           | Data Type | Description |
|-----------------|---------------------------|-----------|-------------|
| accountName     | Your Created Account Name | String    | Account Name|
| activate        | 'Y', 'N'                  | String    | Activate or Deactivate the account|


## Strategies Management

1. [Create Strategy](#create-strategy)
2. [Get Strategies](#get-strategies)
3. [Modify Strategy](#modify-strategy)
4. [Delete Strategy](#delete-strategy)

#### Create Strategy
```python
print(spark.addStrategy(strategyName='Strategy_1',
                        Description='Your Description',
                        StrategyType='Intraday',
                        Display='Private',
                        ForwardTest='N'))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Added.'}
```

#### Get Strategies
```python
print(spark.getStrategy())
```

Sample response
```
{'status': True, 'error': False, 'data': [{'ID': 104, 'StrategyName': 'Your Strategy Name', 'Description': '', 'StrategyType': 'Intraday', 'UCC': 'Your Unique User Code', 'Display': 'Private','ForwardTest': 'N'},{'ID': 127, 'StrategyName': 'Strategy_1', 'Description': 'Your Description', 'StrategyType': 'Intraday', 'UCC': 'Your UCC', 'Display': 'Private', 'ForwardTest': 'Y'}], 'message': 'Strategy data received'}
```

#### Modify Strategy
```python
print(spark.modifyStrategy(strategyName='Strategy_1',
                            Description='Your Description',
                            StrategyType='Intraday',
                            Display='Private',
                            ForwardTest='Y'))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Modified.'}
```

#### Delete Strategy
```python
print(spark.deleteStrategy(strategyName='Strategy_1'))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Deleted.'}
```
| Input Variables | Possible Values         | Data Type | Description                                     |
|-----------------|-------------------------|-----------|-------------------------------------------------|
| strategyName    | Your Strategy Name      | String    | Name of the strategy                           |
| Description     | Your Description        | String    | Description of the strategy                    |
| StrategyType    | 'Intraday', 'Positional' | String    | Type of strategy (Intraday or Positional)      |
| Display         | 'Private', 'Public'     | String    | Display setting for the strategy               |
| ForwardTest     | 'Y', 'N'                | String    | Whether forward testing is enabled             |

## Linked Strategies
1. [Link Account to Strategy](#link-account-to-strategy)
2. [Get Linked Strategies](#get-linked-strategies)
3. [Get Linked Strategies By Account](#get-linked-strategies-by-account)
4. [Modify Linked Strategy](#modify-linked-strategy)
   1. Modify Linked Strategy
   2. Activate Linked Strategy
   3. Deactivate Linked Strategy
5. [Delete Linked Strategy](#delete-linked-strategy)


### Link Account to Strategy
```python
print(spark.addlinkStrategy(strategyName='Your Strategy Name',
                            accountName='Your Account Name',
                            Multiplier=1,
                            Activate='Y',
                            Capital=10000))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Added.'}
```

### Get Linked Strategies
```python
print(spark.getLinkedStrategy())
```

Sample response
```
{'status': True, 'error': False, 'data': [{'StrategyName': 'Your Strategy Name', 'AccountName': 'Your Account Name', 'UCC': 'Your UCC', 'Multiplier': 1, 'Activate': 1, 'Capital': 0.0}], 'message': 'Data Received'}
```

### Get Linked Strategies By Account
```python
print(spark.spark.getlinkStrategyAccount(accountName='Your Account Name'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{'StrategyName': 'Your Strategy Name', 'AccountName': 'Your Account Name', 'UCC': 'Your UCC', 'Multiplier': 1, 'Activate': 1, 'Capital': 0.0}], 'message': 'Data Received'}
```

### Modify Linked Strategy
```python
# Modify Linked Strategy
print(spark.modifyLinkedStrategy(strategyName='Your Strategy Name',
                                accountName='Your Account Name',
                                Multiplier=1,
                                Activate=1,
                                Capital=10000))

# Activate Linked Strategy 
    # Send Activate=1 to activate the strategy
print(spark.modifyLinkedStrategy(strategyName='Your Strategy Name',
                                 accountName='Your Account Name',
                                 Multiplier=1,
                                 Activate=1,
                                 Capital=10000))

# Deactivate Linked Strategy
    # Send Activate=0 to deactivate the strategy
print(spark.modifyLinkedStrategy(strategyName='Your Strategy Name',
                                 accountName='Your Account Name',
                                 Multiplier=1,
                                 Activate=0,
                                 Capital=10000))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Link modified'}
```

### Delete Linked Strategy
```python
 print(spark.deletelinkStrategy(strategyName='Your Strategy Name',accountName='Your Account Name'))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Strategy Link Deleted.'}
```

| Input Variables | Possible Values           | Data Type | Description                                             |
|-----------------|---------------------------|-----------|---------------------------------------------------------|
| strategyName    | Your Strategy Name        | String    | Name of the strategy to be linked/modifed/deleted      |
| accountName     | Your Account Name         | String    | Name of the account to be linked/modifed/deleted       |
| Multiplier      | Integer                   | Integer   | Multiplier value for the linked strategy               |
| Activate        | 'Y', 'N'                  | String    | 'Y' to activate, 'N' to deactivate the linked strategy|
| Capital         | Float                     | Float     | Capital amount for the linked strategy (if applicable) |


## Get Expiy Dates
```python
print(spark.getExpiry(symbol='NIFTY',exchange='NFO',instrument='OPT'))
print(spark.getExpiry(symbol='SBIN',exchange='NFO',instrument='FUT'))
print(spark.getExpiry(symbol='SENSEX',exchange='BFO',instrument='FUT'))
print(spark.getExpiry(symbol='SENSEX',exchange='BFO',instrument='OPT'))
print(spark.getExpiry(symbol='USDINR',exchange='CDS',instrument='FUT'))
```
Sample response
```
{'status': True, 'error': False, 'data': ['2024-04-04', '2024-04-10', '2024-04-18', '2024-04-25'], 'message': 'Data Received'}
```
| Input Variables | Possible Values           | Data Type | Description                          |
|-----------------|---------------------------|-----------|--------------------------------------|
| symbol          | 'NIFTY', 'SBIN', 'SENSEX', etc. | String    | Symbol of the instrument             |
| exchange        | 'NFO', 'BFO', 'CDS','NSE','BSE' | String    | Exchange where the instrument trades |
| instrument      | 'FUT', 'OPT'              | String    | Type of instrument (Futures, Options) |


## Get Tokens
```python
print(spark.getTokens(symbol='NIFTY',exchange='NFO',instrument='OPT',expiry='2024-04-04'))
print(spark.getTokens(symbol='SBIN',exchange='NFO',instrument='FUT',expiry='2024-04-04'))
print(spark.getTokens(symbol='SENSEX',exchange='BFO',instrument='FUT'))
print(spark.getTokens(symbol='SENSEX',exchange='BFO',instrument='OPT'))
print(spark.getTokens(symbol='USDINR',exchange='CDS',instrument='FUT'))
print(spark.getTokens(symbol='SBIN',exchange='NSE'))
print(spark.getTokens(symbol='SBIN',exchange='BSE'))
print(spark.getTokens(symbol='USDINR',exchange='CDS'))
```
Sample response
```
{'status': True, 'error': False, 'data': [{'token': '42378', 'symbol': 'NIFTY04APR2420050PE', 'name': 'NIFTY', 'expiry': '2024-04-04', 'strike': 20050, 'lotsize': 50, 'instrumenttype': 'OPTIDX', 'exch_seg': 'NFO', 'tick_size': 0.05}, {'token': '45720', 'symbol': 'NIFTY04APR2421650PE', 'name': 'NIFTY', 'expiry': '2024-04-04', 'strike': 21650, 'lotsize': 50, 'instrumenttype': 'OPTIDX', 'exch_seg': 'NFO', 'tick_size': 0.05}], 'message': 'Data Received'}
```
| Input Variables | Possible Values         | Data Type | Description                             | Optional |
|-----------------|-------------------------|-----------|-----------------------------------------|----------|
| symbol          | 'NIFTY', 'SBIN', 'SENSEX', etc.| String    | Symbol of the instrument               | No       |
| exchange        | 'NFO', 'BFO', 'CDS', 'NSE', 'BSE' | String | Exchange where the instrument trades   | No       |
| instrument      | 'FUT', 'OPT'            | String    | Type of instrument (Futures, Option)   | Yes      |
| expiry          | Date string format (e.g., '2024-04-04') | String | Expiry date of the instrument (if applicable) | Yes      |


## Order Management
1. [LTP](#LTP)
1. [Place order](#place-an-order)
2. [Modify order](#modify-order)
3. [Cancel order](#cancel-an-order)
4. [Delete Order](#delete-order)
4. [Square off All Orders](#square-off)
5. [Cancel All Orders](#cancel-all-orders)
6. [Stop Operation](#stop-operation)
7. [Square Off Single](#square-off-single)
8. [Delete Trade](#delete-order)
9. [Manual Square Off](#manual-square-off)
10. [Orderbook](#orderbook)
11. [Tradebook](#get-tradebook)
12. [Netposition](#netposition)
13. [Push Trades](#push-trades)

### LTP
Get the last traded price of the token.
Token format is 'Exchange:Token'
```python
print(spark.ltp(Tokens='NSE:212'))
```

### Place an Order
You can place following types of order through this API.
1. Limit Order
2. Market Order
3. SL-Limit Order

Code
```python
# Limit Order
print(spark.place_order(strategyName = "Your Strategy Name",
                                transType = "Buy",
                                token = "NSE:212", 
                                qty = 1,
                                orderType = "Limit",
                                productType = "Intraday",
                                limitPrice = 500.0))
# Market Order
print(spark.place_order(strategyName = "Your Strategy Name",
                               transType = "Sell",
                               token = "NSE:212",
                               qty = 1,
                               orderType = "Market",
                               productType = "Intraday"))
# SL-Limit Order
print(spark.place_order(strategyName = "Your Strategy Name",
                               transType = "Buy",
                               token = "NSE:212",
                               qty = 1,
                               orderType = "SL-Limit",
                               productType = "Intraday",
                               triggerPrice = 220,
                               limitPrice = 210))
```
Sample reponse of place order
```python
{'status': True, 'error': False, 'data': [{'strefID': 2000}], 'message': 'Order Placed.'}
```

| Input Variables | Possible Values                    | Data Type | Description                                           |
|-----------------|------------------------------------|-----------|-------------------------------------------------------|
| strategyName    | "Your Strategy Name", etc.         | String    | Name of the strategy to place the order              |
| transType       | "Buy", "Sell"                      | String    | Type of transaction                                   |
| token           | "NSE:212", etc.                    | String    | Token representing the instrument to trade            |
| qty             | Integer                            | Integer   | Quantity of the instrument to buy/sell               |
| orderType       | "Limit", "Market", "SL-Limit"      | String    | Type of order (Limit, Market, Stop-Limit)            |
| productType     | "Intraday", "Delivery"             | String    | Product type (Intraday, Delivery, etc.)              |
| limitPrice      | Float                              | Float     | Price specified for Limit and SL-Limit orders         |
| triggerPrice    | Float                              | Float     | Trigger price specified for SL-Limit orders          |
| strefID         | Integer                            | Integer   | Reference ID of the order placed                      |

#### Modify Order
```python
print(spark.modify_order(strategyName='Your Strategy Name',
                                strefID=2328,
                                orderType='SL-Limit',
                                limitPrice=190))
```
Sample response of modify order
```
{'status': True, 'error': False, 'data': [{'strefID': 2328}], 'message': 'Order Modified.'}
```

#### Cancel order
```python
print(spark.cancel_order(strategyName='Your Strategy Name',strefID=2328))
```
Sample response of cancel order
```
{'status': True, 'error': True, 'data': [{'strefID': 2328}], 'message': 'Order Cancelled.'}
```

### Delete Order
```python
print(spark.deleteOrder(strefID=2328))
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Order Deleted.'}
```

### Square off
```python
# if you want to square off all orders of Strategy linked to Broker account
print(spark.SquareOff(ctype='strategy',strategyName='Your Strategy Name',accountName='Your Account Name'))
# if you want to square off all orders of Broker account
print(spark.SquareOff(ctype='account',accountName='Your Account Name'))
```

response
```
{'status': True, 'error': False, 'data': [], 'message': 'Request sent to Square Off in StrategyName : Your Strategy Name with AccountName : Your Account Name'}
{'status': True, 'error': False, 'data': [], 'message': 'Request sent to Square Off in AccountNamme : Your Account Name'}
```

### Cancel All Orders
```python
# if you want to cancel all orders of Strategy linked to Broker account
print(spark.CancalAll(ctype='strategy',strategyName='Your Strategy Name',accountName='Your Account Name'))
# if you want to cancel all orders of Broker account
print(spark.CancalAll(ctype='account',accountName='Your Account Name'))
```

response
```
{'status': True, 'error': False, 'data': [], 'message': 'Request sent to Cancel All Orders in StrategyName : Your Strategy Name with AccountName : Your Account Name'}
{'status': True, 'error': False, 'data': [], 'message': 'Request sent to Cancel All Orders in AccountNamme : Your Account Name'}
```

### Stop Operation

```python
print(spark.stopOperation(strategyName='Your Strategy Name', strefID='Your order Reference ID'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{'strefID':Your order Reference ID }], 'message': 'Operations stopped on {Your order Reference ID}'}
```

### Square Off Single
```python
print(spark.squareOffSingle(accountName='You Account Name', strategyName='Your Strategy Name', token='NSE:13528', positionType='Intraday', at_limit='true'))
```

Sample response
```
{"status":true,"error":false,"data":[{"strefID":308}],"message":"Order Placed, Position Squared off"}
```

### Delete Trade
```python
print(spark.deleteTrade(TDno='Your Trade Number'))
```

Sample response
```
{"status":true,"error":false,"data":[],"message":"Trade Deleted"}
```

### Manual Square Off
```python
print(spark.manualSquareOff(accountName='Your Account Name', strategyName='Your Strategy Name', token='NSE:13528', positionType='Intraday', tradedPrice=0, tradedAt=0, ordersPlaced=0, qty=1))
```

Sample response
```
{"status":true,"error":false,"data":[],"message":"Position closed manually."}
```


### orderbook
```python
print(spark.orderbook(accountName='Your Account Name',
                             strategyName='Your Strategy Name',
                             strefid=2000, # Optional : order id
                             reftag='Your Reference Tag', # Optional : Reference Tag    
                             withorders='Y')) # Optional : 'Y' to get all orders 
print(spark.orderbook(accountName='Your Account Name',
                             strategyName='Your Strategy Name'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{'UCC': 'Your UCC', 'AccountName': 'Your Account Name', 'strefID': 2240, 'reftag': 1939, 'StrategyName': 'Your Strategy Name', 'orderType': 'Limit', 'productType': 'Intraday', 'price': 171.25, 'limitPrice': 170.0, 'triggerPrice': 0.0, 'token': 'NSE:212', 'segment': 'C', 'symbol': 'ASHOKLEY-EQ', 'qty': 1, 'transType': 'Buy', 'splitby': 0, 'Operations': '{"timeLimit": 0, "shouldExecute": false, "priceBuffer": 0.0}', 'ordersplaced': 1, 'ordersdone': 1, 'ordersexecuted': 0, 'placed_at': '2024-03-29T10:54:08', 'recon_at': None, 'trade_at': None, 'filledQty': 0, 'tradedQty': None, 'tradeValue': None, 'tradePrice': 0.0, 'status': 'Error', 'active': 0, 'ForwardTest': None}], 'message': 'Data received'}
```

### Get tradebook
```python
print(spark.tradebook(startDate='2024-04-01',
                             endDate='2024-04-03',
                             strategyName='Your Strategy Name',
                             accountName='Your Account Name'))

```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Tradebook data received'}
```

### Netposition
```python
print(spark.netposition(accountName='Your Account Name',
                               strategyName='Your Strategy Name'))
```

Sample response
```
{'status': True, 'error': False, 'data': [{'Your Account Name': {'bookedpnl': 0.816660000000013, 'openpnl': None, 'totalpnl': None, 'totalpositions': 1, 'openpositions': 1}}], 'message': 'Data Received'}
```

### Push Trades
```python
print(spark.pushtrades())
```

Sample response
```
{'status': True, 'error': False, 'data': [], 'message': 'Orders converted to Trades and pushed to database'}
```



### Get Excution Logs
```python
print(spark.excutionLogs())
```

Sample response
```
{'status': True, 'error': True, 'data': [{'Datetime': '2024-04-06T09:36:00', 'UCC': 'Your UCC', 'Name': 'Strategy Name', 'Alert Type': 'DEBUG', 'Message': "Endpoint Called : ModifyOrder. Payload Received : {'UCC': 'Your UCC', 'StrategyName': 'Strategy Name', 'strefID': 2327, 'orderType': 'Market', 'limitPrice': 0.0, 'triggerPrice': 0.0, 'identifier': ''}"}], 'message': 'Data Received'}
```

### Reconnect Order Websocket
```python
print(spark.reconnect_orderWS(accountName='Your Account Name'))
```

Sample response
```
{"status":true,"error":false,"data":[],"message":"Reconnecting to WS"}
```

### Holiday List
```python
print(spark.isHoliday(exch='NFO'))
```

Sample response
```
{'status': True, 'error': False, 'data': ['2024-04-02', '2024-04-06', '2024-04-13', '2024-04-14', '2024-04-19', '2024-04-21', '2024-04-28'], 'message': 'Data Received'}
```

### Freeze Quantity
```python
print(spark.freezeqty(symbol='BANKNIFTY'))
```

Sample response
```
{"status":true,"error":false,"data":[{"freezeQty":900}],"message":"Data Received"}
```

### Contract Master File
```python
print(spark.contractMaster())
```


## Sample Code
```python
from SparkLib import SparkLib

if __name__ == '__main__':
    spark = SparkLib(userid = 'Your User ID', password = 'Your Password', apikeys = 'Your API Keys')
    spark.generate_token()
    print(spark.profile())
    print(spark.placeorder(strategyName = "Your Strategy Name",
                           transType = "Buy",
                           token = "NSE:212",
                           qty = 1,
                           orderType = "SL-Limit",
                           productType = "Intraday",
                           triggerPrice = 220,
                            limitPrice = 210,
                           ))
    print(spark.modifyorder(strategyName='Your Strategy Name',
                            strefID=2328,
                            orderType='SL-Limit',
                            limitPrice=190))
    print(spark.cancelorder(strategyName='Your Strategy Name',strefID=2328))
    print(spark.stopOperation(strategyName='Your Strategy Name', strefID='Your order Reference ID'))
    print(spark.deleteTrade(TDno='Your Trade Number'))
    print(spark.manualSquareOff(accountName='Your Account Name', strategyName='Your Strategy Name', token='NSE:13528', positionType='Intraday', tradedPrice=0, tradedAt=0, ordersPlaced=0, qty=1))    

    print(spark.getExpiry(symbol='NIFTY',exchange='NFO',instrument='OPT'))
    print(spark.getExpiry(symbol='SBIN',exchange='NFO',instrument='FUT'))
    print(spark.getExpiry(symbol='SENSEX',exchange='BFO',instrument='FUT'))
    print(spark.getExpiry(symbol='SENSEX',exchange='BFO',instrument='OPT'))
    print(spark.getExpiry(symbol='USDINR',exchange='CDS',instrument='FUT'))

    print(spark.getTokens(symbol='NIFTY',exchange='NFO',instrument='OPT',expiry='2024-04-04'))
    print(spark.getTokens(symbol='SBIN',exchange='NFO',instrument='FUT',expiry='2024-04-04'))
    print(spark.getTokens(symbol='SENSEX',exchange='BFO',instrument='FUT'))
    print(spark.getTokens(symbol='SENSEX',exchange='BFO',instrument='OPT'))
    print(spark.getTokens(symbol='USDINR',exchange='CDS',instrument='FUT'))
    print(spark.getTokens(symbol='SBIN',exchange='NSE'))
    print(spark.getTokens(symbol='SBIN',exchange='BSE'))
    print(spark.getTokens(symbol='USDINR',exchange='CDS'))

    print(spark.getAllAccounts())
    print(spark.getOneAccount(accountName='Your Account Name'))
    
    print(spark.addStrategy(strategyName='Strategy_1',
                            Description='Your Description',
                            StrategyType='Intraday',
                            Display='Private',
                            ForwardTest='Y'))
    print(spark.getStrategy())
    print(spark.modifyStrategy(strategyName='Strategy_1',Description='Your Description',StrategyType='Intraday',Display='Private',ForwardTest='Y'))
    print(spark.deleteStrategy(strategyName='Strategy_1'))

    # print(spark.addlinkStrategy(strategyName='asa',accountName='Fyers_Vaibhav',Multiplier=1,Activate='Y',Capital=10000))
    print(spark.addlinkStrategy(strategyName='Your Strategy Name',
                                accountName='Your Account Name',
                                Multiplier=1,
                                Activate='Y',
                                Capital=10000))
    print(spark.getlinkStrategy())
    print(spark.modifylinkStrategy(strategyName='Your Strategy Name',accountName='Your Account Name',Multiplier=1,Activate='Y',Capital=10000))
    print(spark.deletelinkStrategy(strategyName='Your Strategy Name',accountName='Your Account Name'))

    print(spark.CancalAll(ctype='strategy',strategyName='Your Strategy Name',accountName='Your Account Name'))
    print(spark.CancalAll(ctype='account',accountName='Your Account Name'))

    print(spark.SquareOff(ctype='strategy',strategyName='Your Strategy Name',accountName='Your Account Name'))
    print(spark.SquareOff(ctype='account',accountName='Your Account Name'))
    print(spark.squareOffSingle(accountName='You Account Name', strategyName='Your Strategy Name', token='NSE:13528', positionType='Intraday', at_limit='true'))
    
    print(spark.ltp(Tokens='NSE:212'))

    print(spark.intradaypnl(strategyName='Your Strategy Name',accountName='Your Account Name'))

    print(spark.netposition(strategyName='Your Strategy Name',accountName='Your Account Name'))
    
    print(spark.pushtrades())

    print(spark.tradebook(startDate='2024-04-01',endDate='2024-04-04',strategyName='Your Strategy Name',accountName='Your Account Name'))

    print(spark.orderbook(strategyName='Your Strategy Name',accountName='Your Account Name'))

    print(spark.excutionLogs())

    print(spark.reconnect_orderWS(accountName='Your Account Name'))
    
    print(spark.isHoliday(exch='NFO'))
    
    print(spark.freezeqty(symbol='BANKNIFTY'))
    
    print(spark.contractMaster())

```



# Order Websocket

The order websocket is a real-time communication protocol used to receive order-related messages from the Spark platform. It provides users with updates on order status, execution, and other pertinent information related to their trading activity. The websocket operates by continuously listening for incoming messages from the Spark platform and relaying them to the user in real-time.

### Features:
- **Real-time Updates**: Receive instant updates on order status and execution as they occur on the Spark platform.
- **Order Status**: Stay informed about any changes in the status of your orders, including pending, executed, or cancelled orders.
- **Execution Details**: Get detailed information about order executions, including trade prices, quantities, and timestamps.
- **Continuous Monitoring**: The websocket continuously listens for incoming order messages, ensuring that users are always up-to-date with their trading activity.

### Usage:
To use the order websocket, users need to establish a connection to the Spark platform using their access token. Once connected, the websocket will start receiving order messages from the platform. Users can define callback functions to handle incoming order messages, error notifications, connection status changes, and more.

### Sample Code:
```python
from maticalgos.sparkLib import SparkLib, OrderSocket

# Define callback functions
def on_order(message):
    print('Order', message)

def on_error(error):
    print('Error', error)

def on_close():
    print("Connection Closed")

def on_open():
    print("Connection Established")

# Create an instance of OrderSocket
order_socket = OrderSocket(
    access_token='Your Access Token',
    on_order=on_order,
    on_error=on_error,
    on_close=on_close,
    on_connect=on_open,
    reconnect=True,             # Optional: Attempt to reconnect if connection is closed
    max_reconnect_attempts=20   # Optional: Maximum attempts to reconnect
)

# Connect to the websocket
order_socket.connect()

# Close the connection
order_socket.close_connection()

```

### Functions:
- **on_order(message)**:
  - This function is called whenever a new order message is received. The message parameter contains the order details, including order status, execution details, and other relevant information.
- **on_error(error)**: 
  - This function is called when an error occurs during the websocket connection.
- **on_close()**: 
  - This function is called when the websocket connection is closed. It can be used to perform cleanup tasks or handle connection closure events.
- **on_open()**: 
  - This function is called when the websocket connection is established. It can be used to perform initialization tasks or handle connection establishment events.
- **reconnect**: 
  - This parameter specifies whether the websocket should attempt to reconnect if the connection is closed. By default, it is set to True.
- **max_reconnect_attempts**: 
  - This parameter specifies the maximum number of reconnection attempts that the websocket should make before giving up. By default, it is set to 20.
- **access_token**: 
  - This parameter specifies the access token required to establish a connection to the Spark platform.
- **connect()**: 
  - This method is used to establish a connection to the Spark platform using the access token.
- **close_connection()**: 
  - This method is used to close the websocket connection to the Spark platform.
  
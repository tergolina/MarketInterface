import json
import base64
import uuid
from datetime import timedelta
from time import sleep
from threading import Event
# Asimov -----------------------------------------------------------------------
from .authentication.bitmex_auth import *
from .connectors.websocket import WebSocket
from .connectors.hot_websocket import HotWebSocket
from .connectors.rest import Rest
from .connectors.poll import Poll
from .types.exchange import Exchange
from .types.utils.utils import *


class Bitmex(Exchange):
    def get_name(self):
        return 'bitmex'

    # Handlers -----------------------------------------------------------------
    def account_handler(self, message):
        json_response = json.loads(message)

        if 'order' in json_response['table']:
            if 'insert' in json_response['action']:
                for order in json_response['data']:
                    std_ord = order_to_standard(self.name, order)
                    self.account.insert_order(std_ord)
                    self.notify('place', update=std_ord, source='websocket')
            elif 'update' in json_response['action']:
                if 'ordStatus' in json_response['data'][0].keys():
                    if 'Canceled' in json_response['data'][0]['ordStatus']:
                        for order in json_response['data']:
                            pair = order["symbol"]
                            std_ord = self.account.get_order(order['orderID'])
                            if std_ord != {}:
                                self.account.remove_order(std_ord)
                            self.notify('cancel', update=std_ord, source='websocket')
                    if 'Filled' in json_response['data'][0]['ordStatus']:
                        for order in json_response['data']:
                            pair = order["symbol"]
                            std_ord = self.account.get_order(order['orderID'])
                            if std_ord != {}:
                                self.account.remove_order(std_ord, from_trade=True)
                        self.notify('execution', update=pair, source='websocket')
                else:
                    for order in json_response['data']:
                        std_ord = self.account.get_order(order['orderID'])
                        if 'price' in order.keys():
                            std_ord['price'] = order['price']
                        if 'quantity' in order.keys():
                            std_ord['quantity'] = order['quantity']
                        if std_ord != {}:
                            self.account.update_order(std_ord['id'], std_ord)
                        self.notify('replace', update=std_ord, source='websocket')

        # Balance
        if "margin" in json_response["table"]:
            balance = {json_response['data'][0]['currency']: {'available':json_response['data'][0]['marginBalance'] / 100000000, 'reserved':0}}
            notify = self.account.set_balance(balance)
            event = 'balance'
            # self.notify('balance', source='websocket')

        # Position
        if "position" in json_response["table"]:
            position = {}
            position_base = {}
            for pos in json_response['data']:
                pair = pos["symbol"]
                position[pair_to_standard(self.name, pair)] = pos['currentQty']
                position_base[pair_to_standard(self.name, pair)] = pos['homeNotional']

            if len(position) > 0:
                self.account.set_position(position)
                self.account.set_position_base(position_base)
                self.notify('position', source='websocket')

    def marketdata_handler(self, message):
        notify = False
        event = None
        update = None
        try:
            json_response = json.loads(message)
            if 'data' in json_response:
                pair = pair_to_standard(self.name, json_response["data"][-1]["symbol"])
                if "trade" in json_response["table"]:
                    event = 'trade'
                    last = {'last': float(json_response["data"][-1]["price"])}
                    self.marketdata.update_market_data(pair, last)
                    notify = True
                    trade = trade_to_standard(self.name, json_response["data"][-1])
                    elapsed = time() - trade['timestamp']
                    update = trade
                if "quote" in json_response["table"]:
                    event = 'book'
                    ticker = {'bid': float(json_response["data"][-1]["bidPrice"]),
                              'ask': float(json_response["data"][-1]["askPrice"])}
                    notify = self.marketdata.update_market_data(pair, ticker)
                    update = {pair: ticker}
                    elapsed = 0
        except Exception as e:
            date_print("bitmex_handler", str(e))
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed)

    def __get_auth_url(self):
        url = "wss://www.bitmex.com/realtime"
        keys, secrets = self.account.get_key()

        """Return auth headers. Will use API Keys if present in parameters."""
        nonce = int(round(time() * 1000))
        values = {
            "api-nonce": str(nonce),
            "api-signature": self.__generate_signature(
                secrets, "GET", "/realtime", nonce, ""
            ),
            "api-key": keys,
        }

        url_values = urllib.parse.urlencode(values)
        url_header = url
        return url_header + "?" + url_values

    def __get_auth_rest_url(self):
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        keys, secrets = self.account.get_key()

        """Return auth headers. Will use API Keys if present in parameters."""
        nonce = int(round(time() * 1000))
        auth = {
            "api-nonce": str(nonce),
            "api-signature": self.__generate_signature(
                secrets, "GET", "/realtime", nonce, ""
            ),
            "api-key": keys,
        }

        url_values = urllib.parse.urlencode(auth)
        url_header = self.account_rest.url
        date_print(url_header + "?" + url_values)
        return url_header + "?" + url_values

    def __get_rest_url(self):
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        return self.account_rest.url

    def __get_rest_auth(self):
        """Return auth headers. Will use API Keys if present in parameters."""
        keys, secrets = self.account.get_key()
        nonce = int(round(time() * 1000))
        auth = {
            "api-nonce": str(nonce),
            "api-signature": self.__generate_signature(
                secrets, "GET", "/realtime", nonce, ""
            ),
            "api-key": keys,
        }
        return auth

    def __generate_signature(self, secret, verb, url, nonce, data):
        """Generate a request signature compatible with BitMEX."""
        # Parse the url so we can remove the base and extract just the path.
        parsedURL = urllib.parse.urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + "?" + parsedURL.query

        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf8")

        message = verb + path + str(nonce) + data
        signature = hmac.new(
            bytes(self.account.secrets[0], "utf8"), bytes(message, "utf8"), digestmod=hashlib.sha256
        ).hexdigest()
        return signature

    def get_marketdata_websocket(self, subs):
        args = []
        if 'book' in subs:
            args += ['quote:'+pair for pair in subs['book']]
        if 'quote' in subs:
            args += ['quote:'+pair for pair in subs['quote']]
        if 'trade' in subs:
            args += ['trade:'+pair for pair in subs['trade']]
        if args != []:
            args = list(set(args))
            subs = {"op": "subscribe", "args" : args}
            if not self.hot:
                return WebSocket("wss://www.bitmex.com/realtime", self.marketdata_handler, [subs])
            else:
                return HotWebSocket("wss://www.bitmex.com/realtime", self.marketdata_handler, [subs])
        else:
            return None

    def get_marketdata_rest(self):
        return Rest('https://www.bitmex.com/api/v1/')

    def get_marketdata_poll(self, subs):
        return None

    def get_account_websocket(self, pairs):
        args = []
        # for pair in pairs:
        #     args += ["order:{}".format(pair)]
        args += ["position", "margin", "order", "quote:ETHUSD"]
        subs = {"op": "subscribe", "args" : args}
        return WebSocket(self.__get_auth_url, self.account_handler, [subs])

    def get_account_rest(self):
        return Rest(self.__get_rest_auth, session=False)

    def get_account_poll(self):
        # polls = {'balance': Poll(self.update_balance, frequency=1/60),
        #          'position': Poll(self.update_position, frequency=1/30),
        #          'open_orders': Poll(self.update_open_orders, frequency=1/30)}
        self.do_update['balance'] = Event()
        self.do_update['position'] = Event()
        self.do_update['open_orders'] = Event()
        polls = {'open_orders': Poll(self.update_open_orders, frequency=1/30, trigger=self.do_update['open_orders'])}
        return polls

    # Trade --------------------------------------------------------------------
    def place_order(self, pair, side, price, quantity, type="placeonly", leverage=1):
        """Place an order.
        limit, limitpegged, market
        """
        keys, secrets = self.account.get_key()
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        self.symbol = pair
        side = side[0].upper() + side[1:].lower()
        endpoint = "order"
        # Generate a unique clOrdID with our prefix so we can identify it.
        clOrdID = base64.b64encode(uuid.uuid4().bytes).decode('utf8').rstrip('=\n')
        if type == 'limit':
            params = {
                'symbol': self.symbol,
                'orderQty': quantity,
                'price': price,
                'side': side,
                'clOrdID': clOrdID
            }
        elif type == 'placeonly':
            params = {
                'symbol': self.symbol,
                'orderQty': quantity,
                'price': price,
                'side': side,
                'clOrdID': clOrdID,
                'execInst': 'ParticipateDoNotInitiate' #, Close
            }
        elif type == 'limitPegged':
            params = {
                'symbol': self.symbol,
                'orderQty': quantity,
                'price': price,
                'side': side,
                'clOrdID': clOrdID,
                'execInst': 'ParticipateDoNotInitiate, Close' #,
            }
        elif type == 'market':
            params = {
                'symbol': self.symbol,
                'orderQty': quantity,
                'side': side,
                'clOrdID': clOrdID,
                'ordType': 'Market'
            }
        elif type == 'marketClose':
            params = {
                'symbol': self.symbol,
                'side': side,
                'clOrdID': clOrdID,
                'ordType': 'Market',
                'execInst': 'Close'
            }
        elif type == 'limitClose':
            params = {
                'symbol': self.symbol,
                'side': side,
                'clOrdID': clOrdID,
                'price': price,
                'execInst': 'Close'
            }
        response = self.account_rest.query(method='post', url=self.account_rest.url + "/order", auth=BitmexAuth(keys, secrets), params=params)
        try:
            order = order_to_standard(self.name, response)
            self.account.insert_order(order) if type != 'market' else None
            self.notify('place', order, source='rest')
            return order
        except:
            update = {'response': str(response), 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': price, 'quantity': quantity}}
            self.notify('error', update, source='rest')
            date_print('Error on order placement. Response: {}'.format(response))
            return response

    def amend_bulk_orders(self, orders):
        """ Amend bulk orders."""
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        keys, secrets = self.account.get_key()
        path = "/order/bulk"
        params = {'orders': json.dumps(orders)}

        response = self.account_rest.query(method='put', url=self.account_rest.url + path, auth=BitmexAuth(keys, secrets), params=params)
        try:
            for ord in response:
                order = order_to_standard(self.name, ord, pair_to_standard(self.name, ord["symbol"]))
                self.account.update_order(order['id'], order, from_trade=False)
            return response
        except:
            update = {'response': str(response), 'call': 'amend_bulk', 'inputs': {'orders': orders}}
            self.notify('error', update, source='rest')
            date_print('Error on amend. Response: {}'.format(response))
            return response

    def create_bulk_orders(self, orders, tag=''):
        """ Amend bulk orders."""
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        keys, secrets = self.account.get_key()
        path = "/order/bulk"
        for order in orders:
            order['clOrdID'] = tag + base64.b64encode(uuid.uuid4().bytes).decode('utf8').rstrip('=\n')
        params = {'orders': json.dumps(orders)}

        response = self.account_rest.query(method='post', url=self.account_rest.url + path, auth=BitmexAuth(keys, secrets), params=params)

        try:
            for ord in response:
                order = order_to_standard(self.name, ord, pair_to_standard(self.name, ord["symbol"]))
                self.account.insert_order(order)
            return response
        except:
            update = {'response': str(response), 'call': 'create_bulk', 'inputs': {'orders': orders}}
            self.notify('error', update, source='rest')
            date_print('Error on create bulk. Response: {}'.format(response))
            return response

    def cancel_order(self, id):
        """Cancel an existing order."""
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        keys, secrets = self.account.get_key()
        path = "/order"
        params = {
            'orderID': id,
        }
        response = self.account_rest.query(method='delete', url=self.account_rest.url + path, auth=BitmexAuth(keys, secrets), params=params)

        try:
            order = order_to_standard(self.name, response[0], pair_to_standard(self.name, response[0]["symbol"]))
            self.account.remove_order(order, from_trade=False)
            return response
        except:
            update = {'response': str(response), 'call': 'cancel', 'inputs': {'id': id}}
            self.notify('error', update, source='rest')
            date_print('Error on cancel. Response: {}'.format(response))
            return response

    def replace_order(self, id, price, quantity=None):
        """Amend an existing order."""
        keys, secrets = self.account.get_key()
        self.account_rest.url = "https://www.bitmex.com/api/v1"
        path = "/order"
        params = {
            'orderID': id,
            'price': price,
        }
        if quantity != None:
            params['orderQty'] = quantity
        response = self.account_rest.query(method='put', url=self.account_rest.url + path, auth=BitmexAuth(keys, secrets), params=params)
        try:
            order = order_to_standard(self.name, response)
            self.account.update_order(order['id'], order, from_trade=False)
            self.notify('replace', order, source='rest')
            return response
        except:
            std_ord = self.account.get_order(params['orderID'])
            update = {'response': str(response), 'call': 'replace_order', 'inputs': {'id': id, 'price': price, 'quantity':quantity, 'pair': std_ord['pair']}}
            self.notify('error', update, source='rest')
            date_print('Error on replace. Response: {}'.format(response))
            return response

    def get_balance(self):
        return self.balance

    def get_open_orders(self):
        url = "https://www.bitmex.com/api/v1"
        command = "/order"
        keys, secrets = self.account.get_key()
        response = self.account_rest.query(method="get", url=url+command, auth=BitmexAuth(keys, secrets), params={"filter": '{"open": true}'})
        if 'error' in response:
            date_print('Error in get_open_orders')
            self.notify('error', str(response['error']['code']), source='rest')
        else:
            open_orders = []
            for raw_order in response:
                open_orders += [order_to_standard(self.name, raw_order)]
            self.account.set_open_orders(open_orders)

    # Snapshot Updates ---------------------------------------------------------
    def update_open_orders(self):
        url = "https://www.bitmex.com/api/v1"
        command = "/order"
        keys, secrets = self.account.get_key()
        response = self.account_rest.query(method="get", url=url+command, auth=BitmexAuth(keys, secrets), params={"filter": '{"open": true}'})
        if 'error' in response:
            date_print('Error in update_open_orders. {}'.format(response))
            self.notify('error', str(response), source='rest')
        else:
            try:
                open_orders = []
                for raw_order in response:
                    open_orders += [order_to_standard(self.name, raw_order)]
                self.account.set_open_orders(open_orders)
                self.notify('orders', source='rest')
            except:
                date_print('UNEXPECTED BUG IN UPDATE_OPEN_ORDERS!')

    def update_positions(self):
        url = "https://www.bitmex.com/api/v1"
        command = "/position"
        keys, secrets = self.account.get_key()

        #filters = json.dumps({"symbol": pair for pair in self.blueprint['account']['pairs']})
        response = self.account_rest.query(method="get", url=url+command, auth=BitmexAuth(keys, secrets))
        print(response)
        if 'error' in response:
            date_print('Error in update_positions. {}'.format(response))
            self.notify('error', str(response), source='rest')
        else:
            try:
                for resp in response:
                    pair = pair_to_standard(self.name, resp["symbol"])
                    position = {pair: resp["currentQty"]}
                    self.account.set_position(position)
                    self.notify('position', source='rest')
            except:
                date_print('UNEXPECTED BUG IN UPDATE_POSITIONS!')

    # Snapshot Requests --------------------------------------------------------
    def get_candles(self, pair, window):
        url = "https://www.bitmex.com/api/v1"
        command = '/trade/bucketed'
        params = {'symbol': pair,
                  'binSize': '1m',
                  'partial': 'false',
                  'count': window,
                  'reverse': 'true'}
        response = self.marketdata_rest.query(method='get', url=url+command, params=params)
        if 'error' not in response:
            candles = []
            for raw_candle in response:
                candles += [{'index': int(datetime.strptime(raw_candle['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()),
                             'close': raw_candle['close']}]
            return candles
        else:
            return response

    def get_trades(self, pair, window):
        url = "https://www.bitmex.com/api/v1"
        command = "/trade"

        start_time = datetime.utcnow() - timedelta(minutes=window+1)
        end_time = datetime.utcnow()
        response = []

        while True:
            params = {'symbol': pair_to_exchange(self.name, pair),
                      "count" : 500,
                      "reverse" : 'false',
                      "startTime": start_time,
                      "endTime": end_time}

            query = self.marketdata_rest.query(method="get", url=url+command, params=params)
            if len(query) > 0:
                response += query
            else:
                break
            start_time = parse(response[-1]["timestamp"]) + timedelta(seconds=1)
            sleep(1)

        if 'error' in response:
            date_print('Error in get_trades')
            self.notify('error', str(response['error']), source='rest')
        else:
            for i, item in enumerate(response):
                response[i] = trade_to_standard(self.name, item, pair_to_exchange(self.name, pair))
            return response

import json
import hashlib
import hmac
import urllib
import base64
from time import sleep
from threading import Event, Lock, Thread
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.websocket import WebSocket
from .connectors.rest import Rest
from .types.exchange import Exchange
from .types.utils.utils import *


class Kraken(Exchange):
    def get_name(self):
        return 'kraken'

    # Handlers -----------------------------------------------------------------
    def marketdata_handler(self, message):
        event = None
        update = None
        notify = False
        elapsed = None
        try:
            data = json.loads(message)

            update = data
            now = time()
            if isinstance(data, dict) and ('event' in data) and (data['event'] == 'subscriptionStatus') and ('status' in data) and (data['status'] == 'subscribed'):
                event = data['subscription']['name']
                self.channels[data['channelID']] = {'event': event, 'pair': data['pair'].replace('XBT', 'BTC')}
            elif isinstance(data, list) and (len(data) >= 1) and (data[0] in self.channels):
                channel = data[0]
                event = self.channels[channel]['event']
                pair = self.channels[channel]['pair']
                if event == 'trade':
                    update = trade_to_standard(self.name, data[1][-1], pair=pair)
                    notify = self.marketdata.update_market_data(pair, {'last': update['price']})
                    elapsed = now - update['timestamp']
                elif event == 'book':
                    if (len(data) == 2) and ('bs' in data[1]) and ('as' in data[1]):
                        raw_book = data[1]
                        entry = {'pair': pair,
                                 'bid': {item[0]: item[1] for item in raw_book['bs']},
                                 'ask': {item[0]: item[1] for item in raw_book['as']}}
                        self.marketdata.queue_entry(entry)
                    else:
                        for raw_book in data[1:]:
                            for raw_side in raw_book:
                                for raw_entry in raw_book[raw_side]:
                                    if len(raw_entry) == 3:
                                        entry = {'pair': pair,
                                                 'side': 'bid' if raw_side == 'b' else 'ask',
                                                 'price': raw_entry[0],
                                                 'quantity': float(raw_entry[1])}
                                        self.marketdata.queue_entry(entry)
        except Exception as e:
            print (self.name, 'marketdata_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update=update, source='websocket', elapsed=elapsed)

    def account_handler(self, message):
        print (message)

    # Connectors ---------------------------------------------------------------
    def get_marketdata_websocket(self, subs):
        if subs:
            s = []
            if 'book' in subs:
                self.channels = {}
                s_ = {'event': 'subscribe',
                     'subscription': {'name': 'book'},
                     'pair': [pair_to_exchange(self.name, p, method='new') for p in subs['book']]}
                s += [s_]

            elif 'trade' in subs:
                self.channels = {}
                s_ = {'event': 'subscribe',
                     'subscription': {'name': 'trade'},
                     'pair': [pair_to_exchange(self.name, p, method='new') for p in subs['trade']]}
                s += [s_]

            return WebSocket('wss://ws.kraken.com', self.marketdata_handler, subs=s)
        else:
            return None

    def get_token(self):
        token = ''
        while True:
            url = 'https://api.kraken.com'
            command = '/0/private/GetWebSocketsToken'
            data, headers = self.kraken_rest_auth(command)
            response = self.account_rest.query(method='post', url=url+command, data=data, headers=headers)
            if 'result' in response and 'token' in response['result']:
                token = response['result']['token']
                break
            else:
                sleep(2)
        return token

    def subscribe_to_open_orders(self):
        token = self.get_token()
        return {"event": "subscribe", "subscription": {"name": "openOrders", "token": token}}

    def subscribe_to_trades(self):
        token = self.get_token()
        return {"event": "subscribe", "subscription": {"name": "ownTrades", "token": token}}

    def get_account_websocket(self, pairs=[]):
        return WebSocket('wss://beta-ws.kraken.com', self.account_handler, subs=[self.subscribe_to_open_orders, self.subscribe_to_trades])

    def get_marketdata_rest(self):
        return Rest('https://api.kraken.com/0/public/Time', session=False)

    def get_marketdata_poll(self, subs):
        polls = []
        # if 'book' in subs:
        #     self.do_update['book'] = {}
        #     p = []
        #     for pair in subs['book']:
        #         self.do_update['book'][pair] = Event()
        #         p += [Poll(self.update_ticker, args=[pair], trigger=self.do_update['book'][pair], frequency=1/5)]
        #     polls += [{'book': p}]
        if 'book' in subs:
            polls = {}
            polls['book'] = {'update': {}, 'reset': {}}
            for pair in subs['book']:
                polls['book']['update'][pair] = Poll(self.book_update, args=[pair])
                # self.do_reset_book[pair] = Event()
                # polls['book']['reset'][pair] = Poll(self.reset_book, args=[pair], trigger=self.do_reset_book[pair], imediate=False)
        return polls

    def get_account_rest(self):
        return Rest('https://api.kraken.com/0/public/Time', session=False)

    def get_order_rest(self):
        return Rest('https://api.kraken.com/0/public/Time', session=False)

    def get_account_poll(self):
        return {'info': Poll(self.update_info, frequency=1/3600, trigger=self.do_update['info']),
                'global_rate_limit': Poll(self.regen_global_rate_limit)}
                # 'position': Poll(self.update_position, trigger=self.do_update['position'], frequency=2, n=3, wait=False),
                # 'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders'], frequency=1, n=3, wait=False)}

    # Trade --------------------------------------------------------------------
    def kraken_rest_auth(self, command, data={}):
        self.account_rest.query_lock.acquire()
        data['nonce'] = nonce()
        key, secret = self.account.get_key()
        encoded = (str(data['nonce']) + urllib.parse.urlencode(data)).encode()
        message = command.encode() + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(signature.digest())
        headers = {'API-Key': key,
                   'API-Sign': sigdigest.decode()}
        self.account_rest.query_lock.release()
        return data, headers

    def place_order(self, pair, side, price, quantity, leverage=5, type=None):
        url = 'https://api.kraken.com'
        command = '/0/private/AddOrder'
        price = self.filter_price(pair, price)
        quantity = self.filter_quantity(pair, quantity)
        if pair in ['BCH/USD', 'BCH/EUR']:
            leverage = min(3, leverage)
        else:
            leverage = min(5, leverage)
        payload = {'pair': pair_to_exchange(self.name, pair),
                   'type': side.lower(),
                   'ordertype': type_to_exchange(self.name, type),
                   'price': price,
                   'volume': quantity,
                   'leverage': leverage}
        if type == 'maker':
            payload['oflags'] = 'post'
        data, headers = self.kraken_rest_auth(command, data=payload)
        if self.can_place(pair=pair):
            response = self.order_rest.query(method='post', url=url+command, data=data, headers=headers, return_elapsed=True)
            self.increase_counter(pair)
            elapsed = response['elapsed']
            response = response['response']
            if 'result' in response:
                if 'txid' in response['result']:
                    if len(response['result']['txid']) > 0:
                        order = {'exchange': self.name,
                                 'id': response['result']['txid'][0],
                                 'pair': pair,
                                 'side': data['type'],
                                 'price': float(price),
                                 'quantity': float(quantity),
                                 'volume': float(price * quantity),
                                 'type': 'margin'}
                        if self.account.insert_order(order):
                            update = order
                            event = 'place'
                            self.notify(event, update, source='rest', elapsed=elapsed)
            elif 'error' in response:
                update = {'response': str(response), 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': float(price), 'quantity': float(quantity), 'leverage': leverage}}
                self.notify('error', update, source='rest', elapsed=elapsed)
        else:
            update = {'response': 'Rate-limit prevention triggered by ' + self.name +' interface', 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': float(price), 'quantity': float(quantity), 'leverage': leverage}}
            self.notify('rate-limit', update, source='rest')

    def replace_order(self, id, price, quantity=None, type='maker'):
        t = Thread(target=self.cancel_order, args=[id])
        t.start()

    def cancel_order(self, id):
        url = 'https://api.kraken.com'
        command = '/0/private/CancelOrder'
        payload = {'txid' : id}
        data, headers = self.kraken_rest_auth(command, data=payload)
        response = self.order_rest.query(method='post', url=url+command, data=data, headers=headers, return_elapsed=True)
        elapsed = response['elapsed']
        response = response['response']
        if 'result' in response:
            if 'count' in response['result']:
                order = self.account.get_order(id)
                if order != {}:
                    if self.account.remove_order(order):
                        update = order
                        event = 'cancel'
                        self.notify(event, update, source='rest', elapsed=elapsed)
        elif 'error' in response:
            update = {'response': str(response), 'call': 'cancel_order', 'inputs': {'id': id}}
            self.notify('error', update, source='rest', elapsed=elapsed)

    # Snapshot Updates ---------------------------------------------------------
    def update_position(self):
        url = 'https://api.kraken.com'
        command = '/0/private/OpenPositions'
        data, headers = self.kraken_rest_auth(command)
        response = self.account_rest.query(method='post', url=url+command, data=data, headers=headers, return_elapsed=True)
        elapsed = response['elapsed']
        response = response['response']
        if 'result' in response:
            position = {}
            for id in response['result']:
                if response['result'][id]['posstatus'] == 'open':
                    pair = pair_to_standard(self.name, response['result'][id]['pair'])
                    pos = float(response['result'][id]['vol']) - float(response['result'][id]['vol_closed'])
                    pos = pos if response['result'][id]['type'] == 'buy' else -pos
                    if pair not in position:
                        position[pair] = 0
                    position[pair] += pos
            self.account.set_position(position)
            self.notify('position', source='rest', elapsed=elapsed)
        elif 'error' in response:
            update = {'response': str(response), 'call': 'update_position', 'inputs': {}, 'key': headers['API-Key']}
            self.notify('error', update, source='rest', elapsed=elapsed)

    def update_open_orders(self):
        url = 'https://api.kraken.com'
        command = '/0/private/OpenOrders'
        data, headers = self.kraken_rest_auth(command)
        response = self.account_rest.query(method='post', url=url+command, data=data, headers=headers, return_elapsed=True)
        elapsed = response['elapsed']
        response = response['response']
        if 'result' in response:
            open_orders = []
            if 'open' in response['result']:
                for id in response['result']['open']:
                    raw_order = response['result']['open'][id]
                    raw_order['id'] = id
                    order = order_to_standard(self.name, raw_order)
                    open_orders += [order]
            self.account.set_open_orders(open_orders)
            self.notify('orders', source='rest', elapsed=elapsed)
        elif 'error' in response:
            update = {'response': str(response), 'call': 'update_open_orders', 'inputs': {}, 'key': headers['API-Key']}
            self.notify('error', update, source='rest', elapsed=elapsed)

    def update_ticker(self, pair):
        if self.marketdata_rest != None:
            url = 'https://api.kraken.com'
            command = '/0/public/Depth'
            data = {'pair': pair_to_exchange('kraken', pair), 'count': 1}
            response = self.marketdata_rest.query(method='post', url=url+command, data=data, return_elapsed=True)
            elapsed = response['elapsed']
            response = response['response']
            if 'result' in response:
                response = response['result']
                raw_book = response[list(response.keys())[0]]
                ticker = {'ask': float(raw_book['asks'][0][0]),
                          'bid': float(raw_book['bids'][0][0])}
                self.marketdata.update_market_data(pair, ticker)
                self.notify('book', {pair: ticker}, source='rest', elapsed=elapsed)
            elif 'error' in response:
                update = {'response': str(response), 'call': 'update_ticker', 'inputs': {'pair': pair}}
                self.notify('error', update, source='rest', elapsed=elapsed)

    # Snapshot Requests --------------------------------------------------------
    def get_candles(self, pair, window):
        url = 'https://api.kraken.com'
        command = '/0/public/OHLC'
        raw_pair = pair_to_exchange(self.name, pair)
        data = {'pair': raw_pair, 'interval': 1}
        response = self.marketdata_rest.query(method='post', url=url+command, data=data)
        if 'result' in response:
            if raw_pair in response['result']:
                candles = []
                for raw_candle in response['result'][raw_pair]:
                    candles += [{'index': int(raw_candle[0]),
                                 'close': float(raw_candle[4])}]
                return candles

    def get_trades(self, pair, window):
        url = 'https://api.kraken.com'
        command = '/0/public/Trades'
        raw_pair = pair_to_exchange(self.name, pair)
        data = {'pair': raw_pair}
        response = self.marketdata_rest.query(method='post', url=url+command, data=data)
        if 'result' in response:
            if raw_pair in response['result']:
                trades = []
                for raw_trade in response['result'][raw_pair]:
                    trades += [trade_to_standard(self.name, raw_trade, pair=pair)]
                return trades

    def get_info(self):
        url = 'https://api.kraken.com'
        command = '/0/public/AssetPairs'
        response = self.account_rest.query(method='get', url=url+command)
        if 'result' in response:
            info = {}
            for raw_pair in response['result']:
                item = response['result'][raw_pair]
                pair = pair_to_standard(self.name, raw_pair)
                p = item['pair_decimals']
                q = item['lot_decimals']
                info[pair] = {'price_filter': ('{:.'+ str(p) + 'f}').format(1/10**p),
                              'quantity_filter': ('{:.'+ str(q) + 'f}').format(1/10**q),
                              'minimum_quantity': '0.02'}
            return info

    # Rate-Limit ---------------------------------------------------------------
    def get_rate_limit(self):
        return {'GLOBAL': 3}

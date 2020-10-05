import json
import hashlib
import hmac
import urllib
from time import sleep, time
from copy import copy, deepcopy
from threading import Event, Lock
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.websocket import WebSocket
from .connectors.rest import Rest
from .types.exchange import Exchange
from .types.utils.utils import *


class Poloniex(Exchange):
    def get_name(self):
        return 'poloniex'

    # Handlers -----------------------------------------------------------------
    def marketdata_handler(self, message):
        event = None
        update = None
        notify = False
        try:
            data = json.loads(message)
            if len(data) > 0:
                if data[0] == 1002:
                    if len(data) >= 3:
                        raw_ticker = data[2]
                        pair = pair_id_to_standard(self.name, raw_ticker[0])
                        ticker = {'last': float(raw_ticker[1]),
                                    'ask': float(raw_ticker[2]),
                                    'bid': float(raw_ticker[3])}
                        notify = self.marketdata.update_market_data(pair, ticker)
                        event = 'book'
                        update = {pair: ticker}
                elif data[0] in POLONIEX_PAIR_ID:
                    if len(data) >= 3:
                        pair = pair_id_to_standard(self.name, data[0])
                        actual = data[1]
                        if (pair in self.book_count) and (self.book_count[pair] != None):
                            if (self.book_count[pair] + 1) == actual:
                                self.book_count[pair] = actual
                            else:
                                self.do_reset_book[pair].set()
                        for raw_update in data[2]:
                            if raw_update[0] == 'i':
                                self.book_count[pair] = actual
                                entry = {'pair': pair,
                                         'bid': raw_update[1]['orderBook'][1],
                                         'ask': raw_update[1]['orderBook'][0]}
                                self.marketdata.queue_entry(entry)
                            elif raw_update[0] == 'o':
                                entry = {'pair': pair,
                                         'side': 'bid' if raw_update[1] == 1 else 'ask',
                                         'price': raw_update[2],
                                         'quantity': float(raw_update[-1])}
                                self.marketdata.queue_entry(entry)
        except Exception as e:
            print (self.name, 'marketdata_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', raw=message)

    def account_handler(self, message):
        event = None
        update = None
        notify = False
        now = time()
        try:
            data = json.loads(message)
            if ('error' in data) and (data['error'] == 'closed'):
                event = 'reset'
                update = {'account': self.name}
                notify = True
            else:
                if len(data) > 0:
                    if data[0] == 1000:
                        if len(data) >= 3:
                            updates = data[2]
                            index = {updates[i][0]:i for i in range(len(updates))}
                            order = None
                            if 'o' in index:
                                o = updates[index['o']]
                                id = int(o[1])
                                order = self.account.get_order(id)
                                if order != {}:
                                    order['quantity'] = float(o[2])
                                    if 't' in index:
                                        raw_trade = updates[index['t']]
                                        trade = copy(order)
                                        trade['id'] = int(raw_trade[1])
                                        trade['quantity'] = float(raw_trade[3])
                                        trade['volume'] = trade['quantity'] * float(raw_trade[2])
                                        funding_type = raw_trade[5] # 0 = exchange, 1 = borrowed, 2 = margin, 3 = lending
                                        if funding_type in [0, 1]:
                                            notify = self.account.update_order(id, order, from_trade=True)
                                            event = trade['side']
                                            update = trade
                                        elif funding_type in [2]:
                                            notify = True
                                            event = 'settlement'
                                    elif order['quantity'] == 0:
                                        notify = self.account.remove_order(order)
                                        event = 'cancel'
                                        update = order
                            if 'n' in index:
                                n = updates[index['n']]
                                type = 'margin'
                                if 'b' in index:
                                    if updates[index['b']][2] == 'e':
                                        type = 'balance'
                                if order != None:
                                    if 'type' in order:
                                        type = order['type']
                                order = order_to_standard(self.name, n, type=type)
                                notify = self.account.insert_order(order)
                                if 'o' in index:
                                    event = 'replace'
                                    update = order
                                else:
                                    event = 'place'
                                    update = order
        except Exception as e:
            print ('account_handler', str(e))
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', raw=message)

    # Connectors ---------------------------------------------------------------
    def get_marketdata_websocket(self, subs):
        # s = []
        # if 'book' in subs:
        #     s += [{'command': 'subscribe', 'channel': 1002}]
        #     self.book_count = {}
        #     for pair in subs['book']:
        #         s += [{'command': 'subscribe', 'channel': pair_to_exchange(self.name, pair)}]
        # if s != []:
        #     return WebSocket('wss://api2.poloniex.com', self.marketdata_handler, subs=s)
        # else:
        #     return None
        return None

    def subscribe_to_account(self):
        key, secret = self.account.get_key()
        n = 'nonce=' + str(nonce())
        sign = hmac.new(secret.encode(), digestmod=hashlib.sha512)
        sign.update(n.encode())
        auth = {'command': 'subscribe',
                'channel': str(1000),
                'key': key,
                'payload': n,
                'sign': sign.hexdigest()}
        return auth

    def get_account_websocket(self, pairs=[]):
        return WebSocket('wss://api2.poloniex.com', self.account_handler, subs=[self.subscribe_to_account])

    def get_marketdata_poll(self, subs):
        polls = {}
        self.do_reset_book = {}
        if 'book' in subs:
            polls = {'book': Poll(self.update_ticker, frequency=4)}
            # polls['book'] = {'update': {}, 'reset': {}}
            # for pair in subs['book']:
            #     self.do_reset_book[pair] = Event()
            #     polls['book']['update'][pair] = Poll(self.book_update, args=[pair])
            #     polls['book']['reset'][pair] = Poll(self.reset_book, args=[pair], trigger=self.do_reset_book[pair], imediate=False)
        return polls

    def get_marketdata_rest(self):
        return Rest('https://poloniex.com/public', session=True)

    def get_account_rest(self):
        return Rest('https://poloniex.com/tradingApi', session=True)

    def get_account_poll(self):
        return {'info': Poll(self.update_info, frequency=1/3600),
                'balance': Poll(self.update_balance, trigger=self.do_update['balance']),
                'position': Poll(self.update_position, trigger=self.do_update['position']),
                'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders']),
                'verify': Poll(self.verify_account_data, frequency=1/10)}

    # Trade --------------------------------------------------------------------
    def __auth(self, command):
        self.account_rest.query_lock.acquire()
        key, secret = self.account.get_key()
        command['nonce'] = nonce()
        payload = urllib.parse.urlencode(command).encode()
        sign = hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest()
        auth = {'Sign': sign,
                'Key': key,
                'Content-Type': 'application/x-www-form-urlencoded'}
        self.account_rest.query_lock.release()
        return payload, auth

    def place_order(self, pair, side, price, quantity, leverage=1, type=None):
        type = 'maker' if type == None else type
        price = int(price * 100000000)/100000000
        quantity = int(quantity * 100000000)/100000000
        command = {'currencyPair': pair_to_exchange(self.name, pair),
                   'rate': price,
                   'amount': quantity}
        if leverage == 0:
            command['command'] = side.lower()

        else:
            command['command'] = 'margin' + side[0].upper() + side[1:].lower()
            command['lendingRate'] = 0.05
        if type == 'sniper':
            command['immediateOrCancel'] = 1
        elif type == 'maker':
            command['postOnly'] = 1
        elif type == 'market':
            if side == 'buy':
                command['rate'] = 100000000
            elif side == 'sell':
                command['rate'] = 0.00000001
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': price, 'quantity': quantity, 'leverage': leverage}}
            self.notify('error', update, source='rest')

    def cancel_order(self, id):
        command = {'command': 'cancelOrder', 'orderNumber': id}
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'cancel_order', 'inputs': {'id': id}}
            self.notify('error', update, source='rest')

    def replace_order(self, id, price, quantity=None):
        command = {'command': 'moveOrder',
                   'orderNumber': id,
                   'rate': int(price * 100000000)/100000000,
                   'postOnly': 1}
        if quantity != None:
            command['amount'] = int(quantity * 100000000)/100000000
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'replace_order', 'inputs': {'id': id, 'price': price, 'quantity': quantity}}
            self.notify('error', update, source='rest')

    def reset_book(self, pair):
        self.logger.log('Interface', 'Reseting book subscription for ' + pair, print_only=True)
        for sub in self.marketdata_websocket.subscriptions:
            if sub['channel'] == pair_to_exchange(self.name, pair):
                self.book_count[pair] = None
                sub['command'] = 'unsubscribe'
                self.marketdata_websocket.send(sub)
                sleep(5)
                sub['command'] = 'subscribe'
                self.marketdata_websocket.send(sub)

    # Snapshot Resquests -------------------------------------------------------
    def get_candles(self, pair, window):
        end = time()
        start = end - ((window + 1) * 60)
        response = self.get_trades(pair, start, end)
        if 'error' in response:
            return response
        else:
            candles = []
            candle_index = (60 * (end // 60)) + 60
            for trade in response:
                index = 60 * (trade['timestamp'] // 60)
                if index < candle_index:
                    candles += [{'index': int(candle_index), 'close': trade['price']}]
                    candle_index = index
            return candles

    def get_trades(self, pair, start, end):
        params = {'command': 'returnTradeHistory',
                  'currencyPair': pair_to_exchange(self.name, pair),
                  'start': start,
                  'end': end}
        response = self.marketdata_rest.query(method='get', params=params)
        if 'error' in response:
            return response
        else:
            trades = []
            for raw_trade in response:
                trades += [trade_to_standard(self.name, raw_trade, pair=pair)]
            return trades

    def get_balance(self):
        command = {'command': 'returnCompleteBalances', 'currencyPair': 'all'}
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_balance', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            balance = {}
            for currency in response:
                balance[currency] = {'available': float(response[currency]['available']),
                                     'reserved': float(response[currency]['onOrders'])}
            return balance

    def get_position(self):
        command = {'command': 'getMarginPosition', 'currencyPair': 'all'}
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_position', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            position = {}
            for pair in response:
                position[pair_to_standard(self.name, pair)] = float(response[pair]['amount'])
            return position

    def get_open_orders(self):
        command = {'command': 'returnOpenOrders', 'currencyPair': 'all'}
        data, headers = self.__auth(command)
        response = self.account_rest.query(method='post', data=data, headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_open_orders', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            order_list = []
            for raw_pair in response:
                pair = pair_to_standard(self.name, raw_pair)
                for raw_order in response[raw_pair]:
                    order_list += [order_to_standard(self.name, raw_order, pair=pair)]
            return order_list

    def get_ticker(self, pair=None):
        url='https://poloniex.com/public?command=returnTicker'
        response = self.marketdata_rest.query(url=url, method='get')
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_ticker', 'inputs': {'pair': pair}}
            self.notify('error', update, source='rest')
        else:
            tickers = {'bid': {}, 'ask': {}}
            for raw_pair in response:
                pair = pair_to_standard(self.name, raw_pair)
                tickers['bid'][pair] = float(response[raw_pair]['highestBid'])
                tickers['ask'][pair] = float(response[raw_pair]['lowestAsk'])
            return tickers

    def get_info(self):
        url = 'https://poloniex.com/public?command=returnTicker'
        response = self.account_rest.query(method='get', url=url)
        if 'error' in response:
            return None
        else:
            info = {}
            for raw_pair in response:
                pair = pair_to_standard(self.name, raw_pair)
                min_quantity = '0.00000001'
                min_tick = '0.00000001'
                info[pair] = {'price_filter': min_tick,
                              'quantity_filter': min_quantity}
            return info

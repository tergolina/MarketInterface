import json
import hashlib
import hmac
import urllib
import base64
import gc
import pandas as pd
from copy import copy
from datetime import datetime, timedelta, timezone
from time import sleep, time
from threading import Thread, Event, Lock, Semaphore
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.rest import Rest
from .connectors.websocket import WebSocket
from .types.exchange import Exchange
from .types.utils.utils import *


class Bitfinex(Exchange):
    def get_name(self):
        return 'bitfinex'

    # Handlers -----------------------------------------------------------------
    def marketdata_handler(self, message):
        event = None
        update = None
        elapsed = None
        timestamp = None
        notify = False
        try:
            data = json.loads(message)
            now = time()
            if 'event' in data:
                if data['event'] == 'subscribed':
                    self.channels[data['chanId']] = {'event': data['channel'], 'pair': pair_to_standard(self.name, data['pair'])}
            elif (isinstance(data, list)):
                if data[1] != 'hb':
                    event = self.channels[data[0]]['event']
                    pair = self.channels[data[0]]['pair']
                    if event == 'trades':
                        if data[1] == 'te':
                            raw_trade = data[-1]
                            if len(raw_trade) >= 4:
                                notify = self.marketdata.update_market_data(pair, {'last': raw_trade[-1]})
                                update = trade_to_standard(self.name, raw_trade, pair=pair)
                                event = 'trade'
                                elapsed = now - update['timestamp']
                    elif event == 'book':
                        if (len(data[1]) > 3) or isinstance(data[1][0], list):
                            entry = {'pair': pair, 'bid': {}, 'ask': {}}
                            raw_book = data[1]
                            for raw_entry in raw_book:
                                if raw_entry[1] != 0:
                                    if raw_entry[2] > 0:
                                        entry['bid'][str(raw_entry[0])] = abs(raw_entry[2])
                                    elif raw_entry[2] < 0:
                                        entry['ask'][str(raw_entry[0])] = abs(raw_entry[2])
                            self.marketdata.queue_entry(entry)
                        elif len(data[1]) == 3:
                            raw_entry = data[1]
                            entry = {'pair': pair,
                                     'side': 'bid' if raw_entry[2] > 0 else 'ask',
                                     'price': str(raw_entry[0]),
                                     'quantity': abs(raw_entry[2]) if raw_entry[1] != 0 else 0}
                            self.marketdata.queue_entry(entry)
                    elif event == 'ticker':
                        raw_ticker = data[1]
                        ticker = {'bid': float(raw_ticker[0]),
                                  'ask': float(raw_ticker[2])}
                        notify = self.marketdata.update_market_data(pair, ticker)
                        update = {pair: ticker}
        except Exception as e:
            print ('bitfinex_handler', str(e))
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed)

    def account_handler(self, message):
        event = None
        update = None
        elapsed = None
        timestamp = None
        notify = False
        try:
            print (message)
            data = json.loads(message)
            if ('error' in data) and (data['error'] == 'closed'):
                event = 'reset'
                update = {'account': self.name}
                notify = True
            elif isinstance(data, list):
                now = time()
                raw_event = data[1]
                if raw_event == 'on':
                    event = 'place'
                    order = order_to_standard(self.name, data[2])
                    notify = self.account.insert_order(order)
                    update = order
                    timestamp = data[2][4]/1000
                    elapsed = now - timestamp
                elif raw_event == 'ou':
                    order = order_to_standard(self.name, data[2])
                    if ('ACTIVE' in data[2][13]) or ('PARTIALLY FILLED' in data[2][13]):
                        event = 'replace'
                        from_trade = False
                        update = order
                        timestamp = data[2][5]/1000
                        elapsed = now - timestamp

                        if 'PARTIALLY FILLED' in data[2][13]:
                            old_order = self.account.get_order(order['id'])
                            if order['fills'] > old_order['fills']:
                                event = 'trade'
                                from_trade = True
                                update = copy(order)
                                update['quantity'] = order['quantity'] - old_order['quantity']

                        notify = self.account.update_order(order['id'], order, from_trade=from_trade)

                elif raw_event == 'oc':
                    order = order_to_standard(self.name, data[2])
                    if 'EXECUTED' in data[2][13]:
                        event = order['side']
                        notify = self.account.remove_order(order, from_trade=True)
                        update = order
                        timestamp = data[2][5]/1000
                        elapsed = now - timestamp
                    elif 'CANCELED' in data[2][13]:
                        event = 'cancel'
                        notify = self.account.remove_order(order, from_trade=False)
                        update = order
                        timestamp = data[2][5]/1000
                        elapsed = now - timestamp
                elif raw_event == 'os':
                    event = 'orders'
                    orders = [order_to_standard(self.name, o) for o in data[2]]
                    notify = self.account.set_open_orders(orders)
                elif raw_event == 'ps':
                    event = 'position'
                    position = {}
                    for raw_position in data[2]:
                        pair = pair_to_standard(self.name, raw_position[0])
                        if pair not in position:
                            position[pair] = 0
                        position[pair] += raw_position[2]
                    notify = self.account.set_position(position)
                # elif raw_event == 'te':
                #     raw_trade = data[2]
                #     trade = trade_to_standard(self.name, raw_trade)
                #     old_order = self.account.get_order(raw_trade[3])
                #     if (old_order != None) and ('id' in old_order):
                #         order = copy(old_order)
                #         order['quantity'] -= trade['quantity']
                #         order['volume'] = order['price'] * order['quantity']
                #         notify = self.account.update_order(old_order['id'], order, from_trade=True)
                #         event = trade['side']
                #         update = trade
                #         timestamp = trade['timestamp']
                #         elapsed = now - timestamp
                elif raw_event == 'n':
                    raw_notification = data[2]
                    if raw_notification[6] in ['ERROR', 'FAILURE']:
                        update = {'response': str(raw_notification[4]), 'message': str(raw_notification[7]), 'call': '', 'inputs': {}}
                        notify = True
                        event = 'error'
        except Exception as e:
            print ('bitfinex_handler', str(e))
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed, raw=message)

    # Auth ---------------------------------------------------------------------
    def subscribe_to_account(self):
        key, secret = self.account.get_key()
        n = nonce()
        a = 'AUTH' + str(n)
        sign = hmac.new(secret.encode(), msg=a.encode(), digestmod=hashlib.sha384).hexdigest()
        payload = {'event': 'auth',
                   'apiKey': key,
                   'authPayload': a,
                   'authSig': sign,
                   'authNonce': n,
                   'dms': 4}
        return payload

    def __auth_v2(self, path, params={}):
        key, secret = self.account.get_key()
        n = str(nonce())
        signature = "/api/" + path + n + json.dumps(params)
        sign = hmac.new(secret.encode('utf8'), signature.encode('utf8'), hashlib.sha384).hexdigest()
        return {"bfx-nonce": n,
                "bfx-apikey": key,
                "bfx-signature": sign,
                "content-type": "application/json"}

    def __auth_v1(self, params={}):
        key, secret = self.account.get_key()
        payload_json = json.dumps(params)
        payload = base64.standard_b64encode(bytes(payload_json, "utf-8"))
        sign = hmac.new(secret.encode('utf8'), payload, hashlib.sha384).hexdigest()
        return {"X-BFX-PAYLOAD": payload,
                "X-BFX-APIKEY": key,
                "X-BFX-SIGNATURE": sign}

    # Connectors ---------------------------------------------------------------
    def get_marketdata_websocket(self, subs):
        s = []
        if 'trade' in subs:
            s += [{'event': 'subscribe', 'channel': 'trades', 'symbol': pair_to_exchange(self.name, p)} for p in subs['trade']]
        if 'book' in subs:
            s += [{'event': 'subscribe', 'channel': 'book', 'len': 100, 'symbol': pair_to_exchange(self.name, p)} for p in subs['book']]
        if 'ticker' in subs:
            s += [{'event': 'subscribe', 'channel': 'ticker', 'symbol': pair_to_exchange(self.name, p)} for p in subs['ticker']]
        if s != []:
            url = 'wss://api.bitfinex.com/ws/2'
            self.channels = {}
            return WebSocket(url, self.marketdata_handler, subs=s)
        else:
            return None

    def get_marketdata_poll(self, subs):
        polls = {}
        # self.do_reset_book = {}
        if 'book' in subs:
            polls['book'] = {'update': {}, 'reset': {}}
            for pair in subs['book']:
                polls['book']['update'][pair] = Poll(self.book_update, args=[pair])
                # self.do_reset_book[pair] = Event()
                # polls['book']['reset'][pair] = Poll(self.reset_book, args=[pair], trigger=self.do_reset_book[pair], imediate=False)
        return polls

    def get_marketdata_rest(self):
        return Rest('https://api.bitfinex.com/v2/')

    def get_account_rest(self):
        return Rest('https://api.bitfinex.com/v2/')

    def get_account_websocket(self, pairs=[]):
        url = 'wss://api.bitfinex.com/ws/2'
        return WebSocket(url, self.account_handler, subs=[self.subscribe_to_account])

    def get_account_poll(self):
        return {'position': Poll(self.update_position, trigger=self.do_update['position']),
                'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders']),
                'info': Poll(self.update_info, frequency=1/3600, trigger=self.do_update['info']),
                'verify': Poll(self.verify_account_data, frequency=1/10)}

    # Trade --------------------------------------------------------------------
    def place_order(self, pair, side, price, quantity, leverage=1, type='maker'):
        price = self.filter_price(pair, price)
        quantity = self.filter_quantity(pair, quantity)
        data = [0, 'on', None,
               {'cid': nonce(),
                'type': type_to_exchange(self.name, type),
                'symbol': 't' + pair_to_exchange(self.name, pair),
                'amount': str(quantity if (side.lower() == 'buy') else - quantity),
                'price': str(price),
                'flags': 4096 if type=='maker' else 0}]
        self.account_websocket.send(data)

    def replace_order(self, id, price, quantity=None, delta=None, type='maker'):
        order = self.account.get_order(id)
        if order != {}:
            pair = order['pair']
            side = order['side']

            price = self.filter_price(pair, price)
            quantity = self.filter_quantity(pair, quantity)
            delta = self.filter_quantity(pair, delta)

            data = [0, 'ou', None, {'id': id, 'price': str(price)}]
            if (delta is not None) and (delta > 0):
                data[3]['delta'] = str(delta)
            elif (quantity is not None) and (quantity != order['quantity']):
                data[3]['amount'] = str(quantity if (side.lower() == 'buy') else - quantity)
            data[3]['flags'] = 4096 if type == 'maker' else 0
            self.account_websocket.send(data)

    def cancel_order(self, id):
        data = [0, 'oc', None, {'id': id}]
        self.account_websocket.send(data)

    def reset_orders(self, pair):
        orders = self.get_open_orders()
        ids = []
        for order in orders:
            if order['pair'] == pair:
                ids += [order['id']]

        if ids != []:
            data = [0, 'oc_multi', None, {'id': ids}]
            self.account_websocket.send(data)
            sleep(0.4)
            self.update_open_orders()

    # Snapshot Requests --------------------------------------------------------
    def get_candles(self, pair, window):
        url = self.marketdata_rest.url
        command = 'candles/trade:1m:t' + pair_to_exchange(self.name, pair) + '/hist'
        params = {'limit': window, 'sort': -1}
        response = self.marketdata_rest.query(method='get', url=url+command, params=params)
        if 'error' not in response:
            candles = []
            for raw_candle in response:
                candles += [{'index': int(raw_candle[0]),
                             'close': raw_candle[2]}]
            return candles
        else:
            return response

    def get_trades(self, pair, window):
        url = self.marketdata_rest.url
        raw_pair = pair_to_exchange(self.name, pair)
        command = 'trades/t' + raw_pair + '/hist'

        start_time = int((datetime.now() - timedelta(minutes=window+1)).timestamp()) * 1000
        end_time = int(datetime.now().timestamp()) * 1000
        response = []

        while True:
            params = {'start': start_time,
                    'end': end_time,
                    'limit' : 1000,
                    'sort' : 1}
            response_ = self.marketdata_rest.query(method='get', url=url+command, params=params)
            if len(response_) <= 1:
                break
            else:
                start_time = response_[-1][1]
                response += response_

        if 'error' not in response:
            for i, item in enumerate(response):
                response[i] = trade_to_standard(self.name, item, pair)
            return response
        else:
            return response

    def get_position(self):
        url = "https://api.bitfinex.com/"
        command = 'v2/auth/r/positions'
        headers = self.__auth_v2(command)
        response = self.account_rest.query(method='post', url=url+command, data=json.dumps({}), headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_position', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            position = {}
            for raw_position in response:
                pair = pair_to_standard(self.name, raw_position[0])
                if pair not in position:
                    position[pair] = 0
                position[pair] += raw_position[2]
            return position

    def get_open_orders(self):
        url = "https://api.bitfinex.com/"
        command = 'v2/auth/r/orders'
        headers = self.__auth_v2(command)
        response = self.account_rest.query(method='post', url=url+command, data=json.dumps({}), headers=headers)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_open_orders', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            return [order_to_standard(self.name, o) for o in response]

    def get_info(self):
        url = 'https://api.bitfinex.com/v1/symbols_details'
        response = self.account_rest.query(method='get', url=url)
        if 'error' in response:
            update = {'response': str(response), 'call': 'get_info', 'inputs': {}}
            self.notify('error', update, source='rest')
        else:
            info = {}
            for item in response:
                pair = pair_to_standard(self.name, item['pair'])
                minimum_quantity = item['minimum_order_size']
                quantity_filter = minimum_quantity[:-1] + '1'
                p = item['price_precision']
                info[pair] = {'price_filter': ('{:.'+ str(p) + 'f}').format(1/10**p),
                              'quantity_filter': quantity_filter,
                              'minimum_quantity': minimum_quantity}
            return info

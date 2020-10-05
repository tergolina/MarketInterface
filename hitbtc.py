import json
import gc
import pandas as pd
import uuid
from datetime import datetime, timedelta, timezone
from time import sleep, time
from threading import Thread, Event, Lock, Semaphore
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.rest import Rest
from .connectors.websocket import WebSocket
from .types.exchange import Exchange
from .types.utils.utils import *


class Hitbtc(Exchange):
    def get_name(self):
        return 'hitbtc'

    # Handlers -----------------------------------------------------------------
    def marketdata_handler(self, message):
        event = None
        update = None
        notify = False
        elapsed = None
        try:
            data = json.loads(message)
            now = time()
            if 'method' in data:
                # Trades -------------------------------------------------------
                if 'trades' in data['method'].lower():
                    trade = data['params']['data'][-1]
                    last = {'last': float(trade['price'])}
                    pair = pair_to_standard(self.name, data['params']['symbol'])
                    notify = self.marketdata.update_market_data(pair, last)
                    event = 'trade'
                    update = trade_to_standard(self.name, trade)
                    timestamp = datetime.strptime(trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
                    elapsed = now - timestamp
                # Book ---------------------------------------------------------
                elif 'ticker' in data['method'].lower():
                    raw_ticker = data['params']
                    ticker = {'bid': float(raw_ticker['bid']),
                              'ask': float(raw_ticker['ask'])}
                    pair = pair_to_standard(self.name, raw_ticker['symbol'])
                    notify = self.marketdata.update_market_data(pair, ticker)
                    event = 'book'
                    update = {pair: ticker}
                    timestamp = datetime.strptime(raw_ticker['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
                    elapsed = now - timestamp
        except Exception as e:
            print (self.name, 'marketdata_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update=update, source='websocket', elapsed=elapsed, raw=message)

    def account_handler(self, message):
        event = None
        update = None
        notify = False
        elapsed = None
        try:
            data = json.loads(message)
            now = time()
            if 'method' in data:
                # Open Orders --------------------------------------------------
                if 'activeOrders' in data['method']:
                    orders = [order_to_standard(self.name, o) for o in data['params']]
                    notify = self.account.set_open_orders(orders)
                    event = 'orders'
                # Report -------------------------------------------------------
                elif 'report' in data['method']:
                    report = data['params']
                    # Trade ----------------------------------------------------
                    if 'trade' in report['reportType']:
                        order = order_to_standard(self.name, report)
                        notify = self.account.update_order(order['id'], order, from_trade=True)
                        trade = trade_to_standard(self.name, report)
                        event = trade['side']
                        update = trade
                    # Place ----------------------------------------------------
                    elif 'new' in report['reportType']:
                        order = order_to_standard(self.name, report)
                        notify = self.account.insert_order(order)
                        event = 'place'
                        update = order
                    # Replace --------------------------------------------------
                    elif 'replaced' in report['reportType']:
                        order = order_to_standard(self.name, report)
                        id = report['originalRequestClientOrderId']
                        notify = self.account.update_order(id, order)
                        event = 'replace'
                        update = order
                    # Cancel ---------------------------------------------------
                    elif 'canceled' in report['reportType']:
                        order = order_to_standard(self.name, report)
                        notify = self.account.remove_order(order)
                        event = 'cancel'
                        update = order
            elif data['id'] == 'balance':
                if 'result' in data:
                    balance = {b['currency']: {'available': float(b['available']), 'reserved': float(b['reserved'])} for b in data['result']}
                    notify = self.account.set_balance(balance)
                    event = 'balance'
            elif data['id'] == 'orders':
                if 'result' in data:
                    orders = [order_to_standard(self.name, o) for o in data['result']]
                    notify = self.account.set_open_orders(orders)
                    event = 'orders'
        except Exception as e:
            print (self.name, 'account_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed, raw=message)

    # Connectors ---------------------------------------------------------------
    def get_marketdata_websocket(self, subs):
        s = []
        if 'trade' in subs:
            for pair in subs['trade']:
                s += [{'method': 'subscribeTrades',
                       'params': {'symbol': pair_to_exchange(self.name, pair)}}]
        if 'book' in subs:
            for pair in subs['book']:
                s += [{'method': 'subscribeTicker',
                       'params': {'symbol': pair_to_exchange(self.name, pair)}}]
        if s != []:
            return WebSocket('wss://api.hitbtc.com/api/2/ws', self.marketdata_handler, subs=s)
        else:
            return None

    def get_account_websocket(self, pairs=[]):
        key, secret = self.account.get_key()
        subs = [{'method': 'login',
                 'params': {'algo': 'BASIC',
                            'pKey': key,
                            'sKey': secret}},
                {'method': 'subscribeReports', 'params': {}},
                {'method': 'getTradingBalance', 'params': {}, 'id': 'balance'}]
        return WebSocket('wss://api.hitbtc.com/api/2/ws', self.account_handler, subs=subs)

    def get_marketdata_rest(self):
        return Rest('https://api.hitbtc.com/api/2', session=False)

    def get_marketdata_poll(self, subs):
        return None

    def get_account_rest(self):
        return Rest('https://api.hitbtc.com/api/2', auth=self.__auth)

    def get_account_poll(self):
        return {'info': Poll(self.update_info, frequency=1/3600),
                'balance': Poll(self.update_balance, trigger=self.do_update['balance'], imediate=False, frequency=1/60),
                'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders'], imediate=False, frequency=1/60)}

    # Trade --------------------------------------------------------------------
    def __auth(self):
        return self.account.get_key()

    def place_order(self, pair, side, price, quantity, leverage=0, type=None):
        type = 'maker' if type == None else type
        url = self.account_rest.url + '/order'
        data = {'symbol': pair_to_exchange(self.name, pair).lower(),
                'side': side,
                'quantity': self.filter_quantity(pair, quantity),
                'price': self.filter_price(pair, price),
                'strictValidate': False,
                'postOnly': type=='maker'}
        if type == 'sniper':
            data['timeInForce'] = 'IOC'
        response = self.account_rest.query(method='post', url=url, data=data)
        if 'error' in response:
            update = {'response': str(response), 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': price, 'quantity': quantity, 'leverage': leverage}}
            self.notify('error', update, source='rest')

    def cancel_order(self, id):
        url = self.account_rest.url + '/order/' + id
        response = self.account_rest.query(method='delete', url=url)
        if 'error' in response:
            update = {'response': str(response), 'call': 'cancel_order', 'inputs': {'id': id}}
            self.notify('error', update, source='rest')

    def replace_order(self, id, price, quantity=None):
        pass

    # Snapshot Updates ---------------------------------------------------------
    def update_balance(self):
        if (self.account_websocket != None) and self.account_websocket.started:
            message = {'method': 'getTradingBalance', 'params': {}, 'id': 'balance'}
            self.account_websocket.send(message)

    def update_open_orders(self):
        if (self.account_websocket != None) and self.account_websocket.started:
            message = {'method': 'getOrders', 'params': {}, 'id': 'orders'}
            self.account_websocket.send(message)

    # Snapshot Requests --------------------------------------------------------
    def get_info(self):
        url = 'https://api.hitbtc.com/api/2/public/symbol'
        response = self.account_rest.query(method='get', url=url)
        if 'error' in response:
            return None
        else:
            info = {}
            for item in response:
                pair = pair_to_standard(self.name, item['id'])
                min_quantity = item['quantityIncrement']
                min_tick = item['tickSize']
                info[pair] = {'price_filter': min_tick,
                              'quantity_filter': min_quantity}
            return info

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
        if self.marketdata_rest != None:
            url = self.marketdata_rest.url
            command = '/public/trades/' + pair_to_exchange(self.name, pair)
            params = {'from': start,
                      'till': end}
            response = self.marketdata_rest.query(method='get', url=url+command, params=params)
            if 'error' in response:
                return response
            else:
                trades = []
                for raw_trade in response:
                    trades += [trade_to_standard(self.name, raw_trade, pair=pair, source='rest')]
                return trades

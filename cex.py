import json
import gc
import pandas as pd
from datetime import datetime, timedelta, timezone
from time import sleep, time
from threading import Thread, Event, Lock, Semaphore
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.rest import Rest
from .connectors.websocket import WebSocket
from .types.exchange import Exchange
from .types.utils.utils import *


class Cex(Exchange):
    def get_name(self):
        return 'cex'

    # Handlers -----------------------------------------------------------------
    def marketdata_handler(self, message):
        event = None
        update = None
        notify = False
        try:
            data = json.loads(message)
            now = time()


        except Exception as e:
            print (self.name, 'marketdata_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update=update, source='websocket', elapsed=elapsed)

    def account_handler(self, message):
        event = None
        update = None
        notify = False
        try:
            data = json.loads(message)
            now = time()


        except Exception as e:
            print (self.name, 'account_handler', str(e), message)
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket')

    # Connectors ---------------------------------------------------------------
    def get_marketdata_websocket(self, subs):
        s = []
        if 'trade' in subs:
            for pair in subs['trade']:
                pass
        if 'book' in subs:
            for pair in subs['book']:
                pass
        if s != []:
            return WebSocket('', self.marketdata_handler, subs=s)
        else:
            return None

    def get_account_websocket(self, pairs=[]):
        subs = [self.__auth]
        return WebSocket('wss://ws.cex.io/ws', self.account_handler, subs=subs)

    def get_marketdata_rest(self):
        return Rest('', session=True)

    def get_marketdata_poll(self, subs):
        return None

    def get_account_rest(self):
        return None

    def get_account_poll(self):
        self.do_update['balance'] = Event()
        self.do_update['position'] = Event()
        self.do_update['open_orders'] = Event()
        return {'balance': Poll(self.update_balance, trigger=self.do_update['balance'], imediate=False),
                'position': Poll(self.update_position, trigger=self.do_update['position'], imediate=False),
                'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders'], imediate=False)}

    # Trade --------------------------------------------------------------------
    def __auth(self):
        key, secret = self.account.get_key()
        return {'e': 'auth',
                'auth': {'key': key,
                         'signature': secret,
                         'timestamp': nonce()}}

    def place_order(self, pair, side, price, quantity, leverage=0):
        pass

    def cancel_order(self, id):
        pass

    def replace_order(self, id, price, quantity):
        order = self.account.get_order(id)
        payload = {'e': 'cancel-replace-order',
                   'data': {'order_id': id,
                            'pair': order['pair'].split('/'),
                            'amount': quantity,
                            'price': str(price),
                            'type': order['side']},
                   'oid': str(nonce())+'_16_cancel-replace-order'}

    # Snapshot Updates ---------------------------------------------------------
    def update_balance(self):
        pass

    def update_open_orders(self):
        pass

    # Snapshot Requests --------------------------------------------------------
    def get_candles(self, pair, window):
        pass

    def get_trades(self, pair, start, end):
        pass

import hashlib
import hmac
import requests
import urllib
import json
import gc
import pandas as pd
from queue import Queue
from datetime import datetime, timedelta, timezone
from time import sleep, time
from threading import Thread, Event, Lock, Semaphore
from decimal import Decimal as D
# Asimov -----------------------------------------------------------------------
from .utils.utils import *
from .account import Account
from .marketdata import MarketData

class Exchange:
    def __init__(self, subscriber=None, blueprint={}, filter=None, logger=None, quick=True, hot=False, book_depth=False, tolerance=0.005/100, no_poll=False):
        self.name = self.get_name()
        self.logger = logger if logger is not None else DummyLogger(self.name)
        # Base Parameters ------------------------------------------------------
        self.blueprint = blueprint
        # Aditional Parameters -------------------------------------------------
        self.no_poll = no_poll
        self.hot = hot
        self.book_depth = book_depth
        self.tolerance = tolerance
        # Subscription ---------------------------------------------------------
        self.notification_filters = []
        self.subscribers = []
        self.n = 0
        self.sub_lock = Lock()
        self.subscribe(subscriber, filter=filter, notify=False)
        # Data -----------------------------------------------------------------
        self.info = {}
        self.marketdata = MarketData()
        self.account = Account()
        # Rate Limit -----------------------------------------------------------
        self.placement_count = {}
        self.rate_limit = self.get_rate_limit()
        # Commands -------------------------------------------------------------
        self.do_update = {i:Event() for i in ['position', 'balance', 'open_orders', 'ticker', 'info']}
        # Connection -----------------------------------------------------------
        self.marketdata_websocket = None
        self.marketdata_rest = None
        self.marketdata_poll = None
        self.account_websocket = None
        self.account_rest = None
        self.account_poll = None
        self.account_verifier = None
        self.connect(blueprint, quick)

    def connect(self, blueprint, quick):
        if 'marketdata' in blueprint:
            # Rest -------------------------------------------------------------
            self.marketdata_rest = self.get_marketdata_rest()
            # Polling ----------------------------------------------------------
            if not self.no_poll:
                self.marketdata_poll = self.get_marketdata_poll(blueprint['marketdata'])
            # WebSocket --------------------------------------------------------
            if blueprint['marketdata'] != {}:
                self.marketdata_websocket = self.get_marketdata_websocket(blueprint['marketdata'])
        if 'account' in blueprint:
            # Credentials ------------------------------------------------------
            keys = blueprint['account']['keys']
            secrets = blueprint['account']['secrets']
            pairs = []
            if 'pairs' in blueprint['account']:
                pairs += blueprint['account']['pairs']
            self.account.set_keys(keys, secrets)
            # Rest -------------------------------------------------------------
            self.account_rest = self.get_account_rest()
            self.order_rest = self.get_order_rest()
            # Polling ----------------------------------------------------------
            if not self.no_poll:
                self.account_poll = self.get_account_poll()
            # WebSocket --------------------------------------------------------
            self.account_websocket = self.get_account_websocket(pairs=pairs)
        if not quick:
            self.barrier()

    def barrier(self):
        sleep(2)
        while True:
            ok = True
            if 'account' in self.blueprint:
                ok = ok and (self.account.orders is not None)
                ok = ok and ((self.account.position is not None) or (self.account.balance is not None))
                if not ok:
                    self.do_update['open_orders'].set()
                    self.do_update['balance'].set()
                    self.do_update['position'].set()
            if ok:
                break
            sleep(5)

    def subscribe(self, subscriber, filter=None, notify=True):
        if callable(subscriber):
            self.sub_lock.acquire()
            self.notification_filters += [filter]
            self.subscribers += [subscriber]
            self.n += 1
            self.sub_lock.release()
            if notify:
                self.notify('subscription')

    def filter_notification(self, state, filter):
        if (filter is None) or (state['event'] == 'subscription'):
            return True
        else:
            event = state['event']
            update = state['update']
            if event in filter:
                if event == 'trade':
                    if update['pair'] in filter[event]:
                        return True
                elif (event == 'book') or (event == 'quote'):
                    ok = False
                    for pair in update:
                        if pair in filter[event]:
                            ok = True
                            break
                    return ok
            elif ('account' in filter):
                if event in ['buy', 'sell', 'place', 'replace', 'cancel']:
                    if update['pair'] in filter['account']:
                        return True
                elif event == 'rate-limit':
                    if ('pair' in update['inputs']) and update['inputs']['pair'] in filter['account']:
                        return True
                else:
                    return True
            return False

    def notify(self, event, update=None, source=None, elapsed=None, raw=''):
        if event is not None:
            state = {'timestamp': time(),
                     'exchange': self.name,
                     'event': event,
                     'update': update,
                     'from': source,
                     'elapsed': elapsed,
                     'account': self.account.get_data(),
                     'marketdata': self.marketdata.get_data(),
                     'info': self.info,
                     'raw': raw}
            message = json.dumps(state)

            self.sub_lock.acquire()
            try:
                i = 0
                while i < self.n:
                    if self.filter_notification(state, self.notification_filters[i]):
                        self.subscribers[i](message)
                    i += 1
            except Exception as e:
                print('NOTIFY ERROR:', str(e), '| subs:', self.subscribers[i], '| event:', event, '| filter:', self.notification_filters[i])
            self.sub_lock.release()

            if (event not in ['verify', 'subscribe']) and (self.logger is not None):
                del state['info']
                self.logger.log('interface', state, silenced=True)

            if event == 'error':
                tag = self.name[0].upper() + self.name[1:]
                print(datetime.now(), '- [', tag, '] Error:', update)

    # Checks -------------------------------------------------------------------
    def has_account(self):
        bp = self.blueprint
        if 'account' in bp:
            if ('keys' in bp['account']) and ('secrets' in bp['account']):
                if (bp['account']['keys'] != []) and (bp['account']['secrets'] != []):
                    return True
        return False

    def has_marketdata_channel(self, channel, pair=None):
        bp = self.blueprint
        if 'marketdata' in bp:
            for c in bp['marketdata']:
                if c == channel:
                    if pair:
                        if pair in bp['marketdata'][channel]:
                            return True
                    else:
                        return True
        return False

    # Virtual Methods ----------------------------------------------------------
    def get_name(self):
        print ('Virtual method "get_name" not declared on child class')
        return ''

    def account_handler(self, message):
        print ('No Account Handler specified for', self.name, ' | Raw message:', message)

    def marketdata_handler(self, message):
        print ('No Market Data Handler specified for', self.name, ' | Raw message:', message)

    def get_marketdata_websocket(self, subs):
        print ('Virtual method "get_marketdata_websocket" not declared on child class')
        return None

    def get_account_websocket(self, pairs=[]):
        print ('Virtual method "get_account_websocket" not declared on child class')
        return None

    def get_account_rest(self):
        print ('Virtual method "get_account_rest" not declared on child class')
        return None

    def get_order_rest(self):
        return None

    def get_marketdata_rest(self):
        print ('Virtual method "get_marketdata_rest" not declared on child class')
        return None

    def get_account_poll(self):
        print ('Virtual method "get_account_poll" not declared on child class')
        return None

    def get_marketdata_poll(self, subs):
        print ('Virtual method "get_marketdata_poll" not declared on child class')
        return None

    # Virtual Basic Commands ---------------------------------------------------
    def place_order(self, pair, side, price, quantity, leverage=0):
        pass

    def cancel_order(self, id):
        pass

    def replace_order(self, id, price, quantity=None):
        pass

    def get_open_orders(self):
        return None

    def get_balance(self):
        return None

    def get_position(self):
        return None

    def get_ticker(self, pair=None):
        return None

    def get_info(self):
        return None

    # Virtual Composite Commands -----------------------------------------------
    def reset_orders(self, pair):
        while True:
            order_list = self.get_open_orders()
            if order_list is None:
                break
            for order in order_list:
                if (order['pair'] == pair) or (pair is None):
                    self.cancel_order(order['id'])
            self.update_open_orders(pair=pair)
            if (pair is None) or ((pair not in self.account.orders) or ((self.account.orders[pair]['bid'] == []) and (self.account.orders[pair]['ask'] == []))):
                break
            sleep(2)

    def update_info(self):
        info = self.get_info()
        if info is not None:
            self.info.update(info)

    def update_ticker(self, pair=None):
        tickers = self.get_ticker()
        if tickers is not None:
            self.marketdata.set_market_data(tickers)
            self.notify('book', source='rest')

    def update_balance(self, pair=None):
        balance = self.get_balance()
        if balance is not None:
            self.account.set_balance(balance, pair=pair)
            self.notify('balance', source='rest')

    def update_position(self, pair=None):
        position = self.get_position()
        if position is not None:
            self.account.set_position(position, pair=pair)
            self.notify('position', source='rest')

    def update_open_orders(self, pair=None):
        open_orders = self.get_open_orders()
        if open_orders is not None:
            self.account.set_open_orders(open_orders, pair=pair)
            self.notify('orders', source='rest')

    def cancel_all_orders(self, pair=None):
        while True:
            order_list = self.get_open_orders()
            if order_list is None:
                return
            for order in order_list:
                if (order['pair'] == pair) or (pair is None):
                    self.cancel_order(order['id'])
            self.update_open_orders(pair)
            if (pair is None) or ((pair not in self.account.orders) or ((self.account.orders[pair]['bid'] == []) and (self.account.orders[pair]['ask'] == []))):
                break
            sleep(2)

    def verify_account_data(self):
        position = self.get_position()
        error = self.account.validate_position(position)
        self.notify('verify', update={'position': error}, source='rest')
        balance = self.get_balance()
        error = self.account.validate_balance(balance)
        self.notify('verify', update={'balance': error}, source='rest')
        open_orders = self.get_open_orders()
        error = self.account.validate_orders(open_orders)
        self.notify('verify', update={'open_orders': error}, source='rest')

    def book_update(self, pair):
        while True:
            if pair not in self.marketdata.book_queue:
                self.marketdata.book_queue[pair] = Queue()
            entry = self.marketdata.book_queue[pair].get()
            if ('pair' in entry) and (pair == entry['pair']):
                if ('bid' in entry) and ('ask' in entry):
                    self.marketdata.set_book(pair, 'bid', entry['bid'])
                    self.marketdata.set_book(pair, 'ask', entry['ask'])
                    notify = True
                else:
                    notify = self.marketdata.insert_entry(entry)
                if notify or self.book_depth:
                    if (pair in self.marketdata.bid) and (pair in self.marketdata.ask):
                        update = {pair: {'bid': self.marketdata.bid[pair],
                                         'ask': self.marketdata.ask[pair],
                                         'bid_quantity': self.marketdata.bid_quantity[pair],
                                         'ask_quantity': self.marketdata.ask_quantity[pair]}}
                        event = 'book'
                        self.notify(event, update, source='websocket')
                        notify = False

    # Filters ------------------------------------------------------------------
    def filter_quantity(self, pair, quantity):
        if (quantity is not None) and (quantity > 0):
            if pair not in self.info:
                self.update_info()
            if 'quantity_filter' not in self.info[pair]:
                self.update_info()
            if (pair in self.info) and ('quantity_filter' in self.info[pair]):
                filter = self.info[pair]['quantity_filter']
                quantity = D(int(quantity / float(filter))) * D(filter)
        return quantity

    def filter_price(self, pair, price):
        if (price is not None) and (price > 0):
            if pair not in self.info:
                self.update_info()
            if 'price_filter' not in self.info[pair]:
                self.update_info()
            if (pair in self.info) and ('price_filter' in self.info[pair]):
                filter = self.info[pair]['price_filter']
                price = D(int(price / float(filter))) * D(filter)
        return price

    # Rate Limit Control -------------------------------------------------------
    def get_rate_limit(self):
        return None

    def regen_global_rate_limit(self):
        while True:
            for item in self.placement_count.keys():
                self.placement_count[item] = max(0, self.placement_count[item] - 1)
            sleep(1)

    def increase_counter(self, pair=None):
        if pair is not None:
            self.placement_count.setdefault(pair, 0)
            self.placement_count[pair] += 1

        if 'SECOND' in self.placement_count:
            self.placement_count['SECOND'] += 1

        if 'HOUR' in self.placement_count:
            self.placement_count['HOUR'] += 1

    def can_place(self, pair):
        return ((self.rate_limit is None)
                or ((self.placement_count.get(pair, 0) <= self.rate_limit.get('GLOBAL', 0))
                    and (self.placement_count.get('HOUR', 0) <= self.rate_limit.get('HOUR', 0))
                    and (self.placement_count.get('SECOND', 0) <= self.rate_limit.get('SECOND', 0))))

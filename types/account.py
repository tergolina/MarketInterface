import hashlib
import hmac
import requests
import urllib
import json
import gc
import pandas as pd
from datetime import datetime, timedelta, timezone
from time import sleep, time
from copy import deepcopy
from threading import Thread, Event, Lock, Semaphore
# Asimov -----------------------------------------------------------------------
from .utils.utils import *


class Account:
    def __init__(self, keys=[], secrets=[]):
        # Credentials ----------------------------------------------------------
        self.keys = keys
        self.secrets = secrets
        self.current_key = 0
        # Data -----------------------------------------------------------------
        self.orders = None
        self.balance = None
        self.position = None
        self.position_base = None
        # Control --------------------------------------------------------------
        self.order_lock = Lock()
        self.balance_lock = Lock()
        self.position_lock = Lock()
        self.position_base_lock = Lock()

    def get_data(self):
        d = {'orders': self.orders,
             'balance': self.balance,
             'position': self.position,
             'position_base': self.position_base}
        return d

    def get_key(self):
        if len(self.keys) > 0:
            self.current_key = (self.current_key + 1) % len(self.keys)
            key = self.keys[self.current_key]
            secret = self.secrets[self.current_key]
            return key, secret

    def is_ready(self):
        return (self.orders is not None) and ((self.balance is not None) or (self.position is not None) or (self.position_base is not None))

    def has_open_orders(self, pair=None, book_side=None):
        if (pair is not None) and (book_side is not None):
            return (pair in self.orders) and (self.orders[pair][book_side] != [])
        else:
            has = False
            orders = deepcopy(self.orders)
            if orders is not None:
                for p in orders:
                    for s in orders[p]:
                        if orders[p][s] != []:
                            has = True
            return has

    def set_keys(self, keys, secrets):
        self.keys = keys
        self.secrets = secrets
        self.current_key = 0

    def set_open_orders(self, orders, pair=None):
        ok = False
        if orders is not None:
            if isinstance(orders, list):
                new_orders = order_list_to_standard(orders)
            else:
                new_orders = orders
            self.order_lock.acquire()
            try:
                if pair == None:
                    self.orders = new_orders
                elif pair in new_orders:
                    self.orders[pair] = new_orders[pair]
                elif pair in self.orders:
                    self.orders[pair] = {'bid': [], 'ask': []}
                ok = True
            except Exception as e:
                print ('set_open_orders', str(e))
            self.order_lock.release()
        return ok

    def set_balance(self, balance, pair=None):
        ok = False
        if balance is not None:
            self.balance_lock.acquire()
            try:
                if pair == None:
                    self.balance = balance
                else:
                    for x in pair.split('/'):
                        if (x in self.balance) and (x in balance):
                            self.balance[x] = balance[x]
                ok = True
            except Exception as e:
                print ('set_balance', str(e))
            self.balance_lock.release()
        return ok

    def set_position(self, position, pair=None):
        ok = False
        if position is not None:
            self.position_lock.acquire()
            try:
                if pair == None:
                    self.position = position
                elif (pair in self.position) and (pair in position):
                    self.position[pair] = position[pair]
                ok = True
            except Exception as e:
                print ('set_position', str(e))
            self.position_lock.release()
        return ok

    def set_position_base(self, position, pair=None):
        ok = False
        if position is not None:
            self.position_base_lock.acquire()
            try:
                if pair == None:
                    self.position_base = position
                elif (pair in self.position_base) and (pair in position):
                    self.position_base[pair] = position[pair]
                ok = True
            except Exception as e:
                print ('set_position_base', str(e))
            self.position_base_lock.release()
        return ok

    # Edit control -------------------------------------------------------------
    # Balance ------------------------------------------------------------------
    def update_balance(self, new_order, old_order=None, from_trade=False, fee_asset=None, absolute_fee=0):
        self.balance_lock.acquire()
        try:
            if '/' in new_order['pair']:
                a, b = new_order['pair'].split('/')
                if not a in self.balance:
                    self.balance[a] = {'available' : 0, 'reserved' : 0}
                if not b in self.balance:
                    self.balance[b] = {'available' : 0, 'reserved' : 0}
                if from_trade:
                    side = new_order['side']
                    if side == 'buy':
                        self.balance[a]['available'] += abs(old_order['quantity'] - new_order['quantity'])
                        self.balance[b]['reserved'] -= abs((old_order['quantity'] * old_order['price']) - (new_order['quantity'] * new_order['price']))
                    else:
                        self.balance[a]['reserved'] -= abs(old_order['quantity'] - new_order['quantity'])
                        self.balance[b]['available'] += abs((old_order['quantity'] * old_order['price']) - (new_order['quantity'] * new_order['price']))
                    fee_asset = (a if side == 'buy' else b) if fee_asset == None else fee_asset
                    if fee_asset in self.balance:
                        self.balance[fee_asset]['available'] -= absolute_fee
                else:
                    if old_order is not None:
                        if new_order['side'] == 'buy':
                            self.balance[b]['available'] -= (new_order['quantity'] * new_order['price']) - (old_order['quantity'] * old_order['price'])
                            self.balance[b]['reserved'] += (new_order['quantity'] * new_order['price']) - (old_order['quantity'] * old_order['price'])
                        elif new_order['side'] == 'sell':
                            self.balance[a]['available'] -= new_order['quantity'] - old_order['quantity']
                            self.balance[a]['reserved'] += new_order['quantity'] - old_order['quantity']
                    else:
                        if new_order['side'] == 'buy':
                            self.balance[b]['available'] -= new_order['quantity'] * new_order['price']
                            self.balance[b]['reserved'] += new_order['quantity'] * new_order['price']
                        elif new_order['side'] == 'sell':
                            self.balance[a]['available'] -= new_order['quantity']
                            self.balance[a]['reserved'] += new_order['quantity']
        except Exception as e:
            print ('update_balance', str(e))
        self.balance_lock.release()

    # Position -----------------------------------------------------------------
    def update_position(self, new_order, old_order):
        self.position_lock.acquire()
        try:
            direction = 1 if new_order['side'] == 'buy' else -1
            pair = new_order['pair']
            if pair not in self.position:
                self.position[pair] = 0
            self.position[pair] += direction * abs(old_order['quantity'] - new_order['quantity'])
        except Exception as e:
            print ('update_position', str(e))
        self.position_lock.release()

    # Orders -------------------------------------------------------------------
    def insert_order(self, order):
        notify = False
        if isinstance(order, dict) and ('pair' in order):
            pair = order['pair']
            self.order_lock.acquire()
            try:
                book_side = 'bid' if order['side'] == 'buy' else 'ask'
                if pair not in self.orders:
                    self.orders[pair] = {'bid': [], 'ask': []}
                ids = [o['id'] for o in self.orders[pair][book_side]]
                if order['id'] not in ids:
                    self.orders[pair][book_side] += [order]
                    notify = True
                    if ('type' in order) and (order['type'] != 'margin'):
                        self.update_balance(order)
            except Exception as e:
                print ('insert_order', str(e))
            self.order_lock.release()
        return notify

    def update_order(self, id, order, from_trade=False, fee_asset=None, absolute_fee=0):
        notify = False
        if isinstance(order, dict) and ('quantity' in order) and ('pair' in order):
            pair = order['pair']
            if order['quantity'] > 0.00000001:
                self.order_lock.acquire()
                try:
                    old = None
                    book_side = 'bid' if order['side'] == 'buy' else 'ask'
                    if pair in self.orders:
                        for i in range(len(self.orders[pair][book_side])):
                            o = self.orders[pair][book_side][i]
                            if o['id'] == id:
                                old = o.copy()
                                if not from_trade:
                                    order['old_id'] = id
                                self.orders[pair][book_side][i] = order
                                notify = True
                                break
                        if old is not None:
                            if ('type' in order) and (order['type'] != 'margin'):
                                self.update_balance(order, old_order=old, from_trade=from_trade, fee_asset=fee_asset, absolute_fee=absolute_fee)
                            elif from_trade:
                                self.update_position(order, old)
                except Exception as e:
                    print ('update_order', str(e))
                self.order_lock.release()
            else:
                notify = self.remove_order(order, from_trade=from_trade)
        return notify

    def remove_order(self, order, from_trade=False, fee_asset=None, absolute_fee=0):
        notify = False
        if isinstance(order, dict) and ('pair' in order):
            pair = order['pair']
            self.order_lock.acquire()
            try:
                i = 0
                old = None
                book_side = 'bid' if order['side'] == 'buy' else 'ask'
                if pair in self.orders:
                    for o in self.orders[pair][book_side]:
                        if o['id'] == order['id']:
                            old = o.copy()
                            break
                        i += 1
                    if old is not None:
                        del self.orders[pair][book_side][i]
                        # gc.collect()
                        notify = True
                        if ('type' in order) and (order['type'] != 'margin'):
                            self.update_balance(order, old_order=old, from_trade=from_trade, fee_asset=fee_asset, absolute_fee=absolute_fee)
                        elif from_trade:
                            self.update_position(order, old)
            except Exception as e:
                print ('remove_order', str(e))
            self.order_lock.release()
        return notify

    def get_order(self, id):
        order = {}
        self.order_lock.acquire()
        try:
            for pair in self.orders:
                for book_side in self.orders[pair]:
                    for o in self.orders[pair][book_side]:
                        if o['id'] == id:
                            order = o.copy()
                            break
        except Exception as e:
            print ('get_order', str(e))
        self.order_lock.release()
        return order

    def get_orders(self, pair, side):
        orders = []
        if pair in self.orders:
            book_side = side
            if book_side == 'buy':
                book_side = 'bid'
            elif book_side == 'sell':
                book_side = 'ask'
            orders = self.orders[pair][book_side]
        return orders

    # Validations --------------------------------------------------------------
    def is_different_enough(self, internal, external):
        if ((internal == 0) and (external != 0)) or ((internal != 0) and (external == 0)):
            return True
        elif (external != 0) and (abs(1 - (internal / external)) > 0.01):
            return True
        else:
            return False

    def validate_position(self, position):
        errors = set()
        if position is not None:
            internal_position = deepcopy(self.position)
            if internal_position is not None:
                for pair in internal_position:
                    if (pair not in position) or self.is_different_enough(internal_position[pair], position[pair]):
                        errors = errors | {pair}
                for pair in position:
                    if (pair not in internal_position) or self.is_different_enough(internal_position[pair], position[pair]):
                        errors = errors | {pair}
        return list(errors)

    def validate_balance(self, balance):
        errors = set()
        if balance is not None:
            internal_balance = deepcopy(self.balance)
            if internal_balance is not None:
                for currency in internal_balance:
                    internal_available = internal_balance[currency]['available']
                    internal_reserved = internal_balance[currency]['reserved']
                    external_available = balance[currency]['available']
                    external_reserved = balance[currency]['reserved']
                    if ((currency not in balance)
                        or self.is_different_enough(internal_available, external_available)
                        or self.is_different_enough(internal_reserved, external_reserved)):
                        errors = errors | {currency}
                for currency in balance:
                    if ((currency not in internal_balance)
                        or self.is_different_enough(internal_available, external_available)
                        or self.is_different_enough(internal_reserved, external_reserved)):
                        errors = errors | {currency}
        return list(errors)

    def validate_orders(self, orders_list):
        errors = set()
        if orders_list is not None:
            internal_orders = deepcopy(self.orders)
            if internal_orders is not None:
                # Standard to list ---------------------------------------------
                internal_orders_list = []
                for pair in internal_orders:
                    for book_side in internal_orders[pair]:
                        internal_orders_list += internal_orders[pair][book_side]
                # Compare ID ---------------------------------------------------
                internal_id_list = [o['id'] for o in internal_orders_list]
                id_list = [o['id'] for o in orders_list]
                for order in orders_list:
                    if order['id'] not in internal_id_list:
                        errors = errors | {order['pair']}
                for order in internal_orders_list:
                    if order['id'] not in id_list:
                        errors = errors | {order['pair']}
                # Compare Quantity and Price -----------------------------------
                for order1 in orders_list:
                    for order2 in internal_orders_list:
                        if order1['id'] == order2['id']:
                            if (order1['price'] != order2['price']) or self.is_different_enough(order1['quantity'], order2['quantity']):
                                errors = errors | {order['pair']}
        return list(errors)

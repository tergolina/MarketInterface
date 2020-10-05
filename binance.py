import json
import gc
import pandas as pd
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from time import sleep, time
from threading import Thread, Event, Lock, Semaphore
# Asimov -----------------------------------------------------------------------
from .connectors.poll import Poll
from .connectors.rest import Rest
from .connectors.websocket import WebSocket
from .types.exchange import Exchange
from .types.utils.utils import *


class Binance(Exchange):
    def get_name(self):
        return 'binance'

    # Handlers -----------------------------------------------------------------
    def account_handler(self, message):
        event = None
        update = None
        elapsed = None
        timestamp = None
        notify = False
        raw = None
        try:
            data = json.loads(message)
            now = time()
            if ('e' in data) and (data['e'] == 'executionReport'):
                order_status = data['X']
                execution_type = data['x']
                if execution_type == 'NEW':
                    event = 'place'
                    order = order_to_standard(self.name, data)
                    notify = self.account.insert_order(order)
                    update = order
                elif execution_type == 'CANCELED':
                    event = 'cancel'
                    order = order_to_standard(self.name, data)
                    order['quantity'] = 0
                    notify = self.account.remove_order(order, from_trade=False)
                    update = order
                elif execution_type == 'TRADE':
                    order = order_to_standard(self.name, data)
                    old_order = self.account.get_order(order['id'])
                    if 'price' in old_order and old_order['price'] != order['price']:
                        old_order['price'] = order['price']
                        notify = self.account.update_order(old_order['id'], old_order, from_trade=False)
                    if order_status == 'FILLED':
                        fee_asset = data['N']
                        absolute_fee = float(data['n'])
                        event = order['side']
                        notify = self.account.remove_order(order, from_trade=True, fee_asset=fee_asset, absolute_fee=absolute_fee)
                        update = order
                    elif order_status == 'PARTIALLY_FILLED':
                        fee_asset = data['N']
                        absolute_fee = float(data['n'])
                        event = order['side']
                        notify = self.account.update_order(order['id'], order, from_trade=True, fee_asset=fee_asset, absolute_fee=absolute_fee)
                        update = order
            elif ('e' in data) and (data['e'] == 'outboundAccountInfo'):
                if 'B' in data:
                    balance = {}
                    for b in data['B']:
                        raw_currency = b['a']
                        available = float(b['f'])
                        reserved = float(b['l'])
                        if available + reserved > 0:
                            balance[pair_to_standard(self.name, raw_currency)] = {'available': available, 'reserved': reserved}
                    notify = self.account.set_balance(balance)
                    notify = False
                    event = 'balance'
        except Exception as e:
            print ('binance_handler', str(e))
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed, raw='')

    def marketdata_handler(self, message):
        event = None
        update = None
        elapsed = None
        timestamp = None
        notify = False
        try:
            data = json.loads(message)
            now = time()
            if 'e' in data:
                # Market Data --------------------------------------------------
                # Trade --------------------------------------------------------
                if data['e'] == 'aggTrade':
                    if ('s' in data) and ('p' in data) and ('T' in data):
                        event = 'trade'
                        trade = trade_to_standard(self.name, data)
                        pair = trade["pair"]
                        last = {'last': trade['price']}
                        notify = self.marketdata.update_market_data(pair, last, side=trade['side'], tolerance=self.tolerance)
                        timestamp = data['T']/1000
                        elapsed = now - timestamp
                        update = trade
                elif data['e'] == '24hrTicker':
                    if ('s' in data) and ('p' in data) and ('E' in data):
                        event = 'book'
                        pair = pair_to_standard(self.name, data['s'])
                        ticker = {'bid': float(data['b']),
                                  'ask': float(data['a'])}
                        notify = self.marketdata.update_market_data(pair, ticker)
                        timestamp = data['E']/1000
                        elapsed = now - timestamp
                        update = {pair: ticker}
        except Exception as e:
            print ('binance_handler', str(e))
        # Notify ---------------------------------------------------------------
        if notify:
            self.notify(event, update, source='websocket', elapsed=elapsed)

    # Trade --------------------------------------------------------------------
    def __auth(self, params, key=None, secret=None):
        if key is None or secret is None:
            key, secret = self.account.get_key()
        q = '&'.join(['{}={}'.format(item, params[item]) for item in params])
        m = hmac.new(secret.encode('utf-8'), q.encode('utf-8'), hashlib.sha256)
        params['signature'] = m.hexdigest()
        return params

    # Connectors ---------------------------------------------------------------
    def websocket_auth(self):
        while True:
            url = 'https://api.binance.com/api/v1/'
            command = 'userDataStream'
            key, secret = self.account.get_key()
            headers = {'Accept': 'application/json',
                       'User-Agent': 'binance/python',
                       'X-MBX-APIKEY': key}
            response = self.account_rest.query(method='post', url=url+command, headers=headers)
            if 'error' not in response:
                break
            sleep(5)
        self.info['listenKey'] = response['listenKey']
        return 'wss://stream.binance.com:9443/ws/' + self.info['listenKey']

    def get_account_websocket(self, pairs=[]):
        return WebSocket(self.websocket_auth, self.account_handler)

    def get_account_poll(self):
        return {'rate_limit': Poll(self.regen_rate_limit),
                'balance': Poll(self.update_balance, trigger=self.do_update['balance']),
                'info': Poll(self.update_info, frequency=1/3600, trigger=self.do_update['info']),
                'put_listenkey': Poll(self.put_listenkey, imediate=False, frequency=1/600),
                'open_orders': Poll(self.update_open_orders, trigger=self.do_update['open_orders']),
                'verify': Poll(self.verify_account_data, frequency=1/120)}

    def get_marketdata_websocket(self, subs):
        s = []
        if 'trade' in subs:
            s += [pair_to_exchange(self.name, p).lower() + '@aggTrade' for p in subs['trade']]
        if 'book' in subs:
            s += [pair_to_exchange(self.name, p).lower() + '@ticker' for p in subs['book']]
        if 'quote' in subs:
            s += [pair_to_exchange(self.name, p).lower() + '@ticker' for p in subs['quote']]
        if s != []:
            url = 'wss://stream.binance.com:9443/ws/' + '{}'.format('/'.join(s))
            return WebSocket(url, self.marketdata_handler)
        else:
            return None

    def get_marketdata_poll(self, subs):
        return None

    def get_marketdata_rest(self):
        return Rest('https://api.binance.com', session=False)

    def get_account_rest(self):
        return Rest('https://api.binance.com', session=True)

    def put_listenkey(self):
        if 'listenKey' in self.info:
            url = 'https://api.binance.com/api/v1/'
            command = 'userDataStream'
            key, secret = self.account.get_key()
            headers = {'Accept': 'application/json',
                       'User-Agent': 'binance/python',
                       'X-MBX-APIKEY': key}
            params = {'listenKey' : self.info['listenKey']}
            response = self.account_rest.query(method='put', url=url+command, headers=headers, data=params)
            if 'error' in response or response != {}:
                update = {'message': error_to_standard(self.name, response), 'call': 'put_listenkey', 'inputs': params}
                self.notify('error', update, source='rest', raw=str(response))

    # Snapshot Requests --------------------------------------------------------
    def get_pairs(self):
        pairs = []
        url = self.marketdata_rest.url
        command = '/api/v1/ticker/24hr'
        response = self.marketdata_rest.query(method='get', url=url+command)
        if 'error' not in response:
            for item in response:
                pairs += [pair_to_standard(self.name, item['symbol'])]
        return pairs

    def get_ticker(self, pair):
        url = self.marketdata_rest.url
        command = '/api/v1/ticker/24hr'
        params = {'symbol': pair_to_exchange(self.name, pair)}
        response = self.marketdata_rest.query(method='get', url=url+command, params=params)
        if 'error' not in response:
            return {'bid': float(response['bidPrice']),
                    'ask': float(response['askPrice'])}
        else:
            return response

    def get_candles(self, pair, window, candle_size='1m'):
        url = self.marketdata_rest.url
        command = '/api/v1/klines'
        params = {'symbol': pair_to_exchange(self.name, pair),
                  'interval': candle_size,
                  'limit': window}
        response = self.marketdata_rest.query(method='get', url=url+command, params=params)
        if 'error' not in response:
            candles = []
            for raw_candle in response:
                candles += [{'index': (int(raw_candle[6]) + 1)//1000,
                             'close': float(raw_candle[4]),
                             'volume': float(raw_candle[7]),
                             'trades': raw_candle[8]}]
            return candles
        else:
            return response

    def get_trades(self, pair, window):
        url = self.marketdata_rest.url
        command = '/api/v1/aggTrades'
        start_time = int((time() - (window + 1)*60)*1000)
        end_time = int(time()*1000)
        response = []
        query = []
        diff_time = 0
        first_time = True

        while (diff_time < window*60*1000):
            if not first_time:
                sleep(1/20)
                start_time = int((time() - (window + 1)*60)*1000)
                end_time = int(response[0]["T"])
            params = {'symbol': pair_to_exchange(self.name, pair),
                      'limit': 1000,
                      'startTime': start_time,
                      'endTime': end_time}

            query = self.marketdata_rest.query(method='get', url=url+command, params=params)

            # Remove duplicate trades
            if isinstance(query, list):
                if first_time:
                    response = query + response
                else:
                    if (int(query[-1]["a"]) < int(response[0]["a"])):
                        response = query + response
                    else:
                        #has ID intersection
                        for item in reversed(query):
                            if int(item["a"]) >= int(response[0]["a"]):
                                query.remove(item)
                            else:
                                break
                        response = query + response
                diff_time = int(time()*1000) - int(response[0]["T"])
                first_time = False

        for i, item in enumerate(response):
            diff_time = float(response[-1]["T"]) - float(item["T"])
            if ((diff_time/60/1000) > window):
                # print("REMOVED item: ", datetime.utcfromtimestamp(float(item["T"])/1000))
                response.remove(item)
            else:
                break
        for i, item in enumerate(response):
            response[i] = trade_to_standard(self.name, item, pair=pair, source='rest')
        return response

    def get_open_orders(self, pair=None):
        url = self.account_rest.url
        command = '/api/v3/openOrders'
        params = {'timestamp' : int(time() * 1000)}
        if pair is not None:
            params['symbol'] = pair_to_exchange(self.name, pair)
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        response = self.account_rest.query(method='get', url=url+command, params=params, headers={'X-MBX-APIKEY' : key})
        if 'error' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_open_orders', 'inputs': {}}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            return [{'exchange': self.name,
                     'id': resp['orderId'],
                     'pair': pair_to_standard(self.name, resp['symbol']),
                     'side': resp['side'].lower(),
                     'price': float(resp['price']),
                     'quantity': float(resp['origQty']) - float(resp['executedQty']),
                     'volume': float(resp['price']) * (float(resp['origQty']) - float(resp['executedQty'])),
                     'type': 'balance'} for resp in response]

    def get_order_status(self, order):
        url = self.account_rest.url
        command = '/api/v3/order'
        params = {'timestamp' : int(time() * 1000),
                  'symbol' : pair_to_exchange(self.name, order['pair']),
                  'orderId' : order['id'],}
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        response = self.account_rest.query(method='get', url=url+command, params=params, headers={'X-MBX-APIKEY' : key})
        if 'error' in response or not 'status' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_order_status', 'inputs': params}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            return response['status']

    def get_balance(self):
        url = self.account_rest.url
        command = '/api/v3/account'
        params = {'timestamp' : int(time() * 1000)}
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        response = self.account_rest.query(method='get', url=url+command, params=params, headers={'X-MBX-APIKEY' : key})
        if 'error' in response or not 'balances' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_balance', 'inputs': {}}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            balance = {}
            for item in response['balances']:
                pair = item['asset']
                pair = pair_to_standard(self.name, pair)
                available = float(item['free'])
                reserved = float(item['locked'])
                if (available + reserved) > 0:
                    balance[pair] = {'available': available,
                                     'reserved': reserved}
            return balance

    def get_info(self):
        command = '/api/v1/exchangeInfo'
        if 'marketdata' in self.blueprint:
            url = self.marketdata_rest.url
            response = self.marketdata_rest.query(method='get', url=url+command, params={})
        elif 'account' in self.blueprint:
            url = self.account_rest.url
            response = self.account_rest.query(method='get', url=url+command, params={})
        if 'error' not in response:
            info = {}
            for item in response['symbols']:
                if 'filters' in item and 'symbol' in item:
                    pair = pair_to_standard(self.name, item['symbol'])
                    info[pair] = {'price_filter': '0.00000001',
                                  'quantity_filter': '0.00000001',
                                  'minimum_quantity': '0.00000001',
                                  'minimum_volume': '0.00000001'}
                    for filter in item['filters']:
                        if 'filterType' in filter and filter['filterType'] == 'LOT_SIZE':
                            info[pair]['minimum_quantity'] = filter['minQty']
                            info[pair]['quantity_filter'] = filter['stepSize']
                        elif 'filterType' in filter and filter['filterType'] == 'PRICE_FILTER':
                            info[pair]['price_filter'] = filter['tickSize']
                        elif 'filterType' in filter and filter['filterType'] == 'MIN_NOTIONAL':
                            info[pair]['minimum_volume'] = filter['minNotional']
            return info

    def get_quantity_filter(self):
        url = self.account_rest.url
        command = '/api/v1/exchangeInfo'
        response = self.account_rest.query(method='get', url=url+command, params={})
        if 'error' in response or not 'symbols' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_quantity_filter', 'inputs': {}}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            lot_size_filter = {}
            for item in response['symbols']:
                if 'filters' in item and 'symbol' in item:
                    pair = pair_to_standard(self.name, item['symbol'])
                    for sub_item in item['filters']:
                        if 'filterType' in sub_item and sub_item['filterType'] == 'LOT_SIZE':
                            lot_size_filter[pair] = sub_item['minQty']
            self.info['quantity_filter'] = lot_size_filter

    def get_price_filter(self):
        url = self.account_rest.url
        command = '/api/v1/exchangeInfo'
        response = self.account_rest.query(method='get', url=url+command, params={})
        if 'error' in response or not 'symbols' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_quantity_filter', 'inputs': {}}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            lot_size_filter = {}
            for item in response['symbols']:
                if 'filters' in item and 'symbol' in item:
                    pair = pair_to_standard(self.name, item['symbol'])
                    for sub_item in item['filters']:
                        if 'filterType' in sub_item and sub_item['filterType'] == 'PRICE_FILTER':
                            lot_size_filter[pair] = sub_item['tickSize']
            self.info['price_filter'] = lot_size_filter

    def get_subaccount_balance(self, account):
        url = self.account_rest.url
        command = '/wapi/v3/sub-account/assets.html'
        params = {'email': account,
                  'timestamp': int(time() * 1000),
                  }
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        url = url + command + '?' + '&'.join([str(r)+'='+str(params[r]) for r in params])
        response = self.account_rest.query(method='get', url=url, headers={'X-MBX-APIKEY': key})
        if 'error' in response or not 'balances' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'get_subaccount_balance', 'inputs': {'account': account}, 'params': params}
            self.notify('error', update, source='rest', raw=str(response))
        else:
            balance = {}
            for item in response['balances']:
                pair = item['asset']
                pair = 'STR' if pair == 'XLM' else pair
                pair = 'BCH' if pair == 'BCC' else pair
                available = float(item['free'])
                reserved = float(item['locked'])
                if (available + reserved) > 0:
                    balance[pair] = {'available': available,
                                     'reserved': reserved}
            return balance

    # Transfer -----------------------------------------------------------------
    def transfer_assets(self, asset, quantity, source, destiny):
        url = self.account_rest.url
        command = '/wapi/v3/sub-account/transfer.html'
        params = {'fromEmail': source, 'toEmail': destiny, 'asset': pair_to_exchange(self.name, asset), 'amount': quantity, 'timestamp' : int(time() * 1000)}
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        url = url + command + '?' + '&'.join([str(r)+'='+str(params[r]) for r in params])
        response = self.account_rest.query(method='post', url=url, headers={'X-MBX-APIKEY' : key})
        if 'error' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'transfer_assets', 'inputs': {'asset': asset, 'quantity': quantity, 'source': source, 'destiny': destiny}}
            self.notify('error', update, source='rest', raw=str(response))
        return response

    # Trade --------------------------------------------------------------------
    def place_order(self, pair, side, price, quantity, leverage=0, type='maker'):
        price = self.filter_price(pair, price)
        quantity = self.filter_quantity(pair, quantity)

        url = self.account_rest.url
        command = '/api/v3/order'
        params = {'timestamp' : int(time() * 1000),
                  'symbol' : pair_to_exchange(self.name, pair),
                  'side' : side.upper(),
                  'type' : 'MARKET' if type=='market' else 'LIMIT',
                  'quantity' : str(quantity)}

        if type != 'market':
            params['price'] = str(price)
            params['timeInForce'] = 'GTC'

        if self.can_place(pair=pair):
            key, secret = self.account.get_key()
            params = self.__auth(params, key=key, secret=secret)
            response = self.account_rest.query(method='post', url=url+command, data=params, headers={'X-MBX-APIKEY' : key})
            self.increase_counter()
            if 'error' in response:
                update = {'message': error_to_standard(self.name, response), 'call': 'place_order', 'inputs': params}
                self.notify('error', update, source='rest', raw=str(response))
                print (self.account.balance)
            # else:
            #     if float(response['executedQty']) == 0:
            #         order = order_to_standard(self.name, response, source='rest')
            #         if self.account.insert_order(order):
            #             self.notify('place', order, source='rest', raw=str(response))
            return response
        else:
            update = {'response': 'Rate-limit prevention triggered by ' + self.name +' interface', 'call': 'place_order', 'inputs': {'pair': pair, 'side': side, 'price': float(price), 'quantity': float(quantity), 'leverage': leverage}}
            self.notify('rate-limit', update, source='rest')

    def replace_order(self, id, price, quantity=None, type='maker'):
        t = Thread(target=self.cancel_order, args=[id])
        t.start()

    def cancel_order(self, id, pair=None):
        if pair == None:
            order = self.account.get_order(id)
            if order == {}:
                return []
            pair = order['pair']
        url = self.account_rest.url
        command = '/api/v3/order'
        params = {'timestamp' : int(time() * 1000),
                  'orderId' : id,
                  'symbol' : pair_to_exchange(self.name, pair)}
        key, secret = self.account.get_key()
        params = self.__auth(params, key=key, secret=secret)
        response = self.account_rest.query(method='delete', url=url+command, data=params, headers={'X-MBX-APIKEY' : key})
        if 'error' in response:
            update = {'message': error_to_standard(self.name, response), 'call': 'cancel_order', 'inputs': params}
            self.notify('error', update, source='rest', raw=str(response))

    # Rate Limit ---------------------------------------------------------------
    def get_rate_limit(self):
        #the API order limit is 10 orders/second and 100,000 orders/24 hours.
        return {'HOUR': 4100, 'SECOND': 10, 'GLOBAL': 10000000}

    def regen_rate_limit(self):
        #the API order limit is 10 orders/second and 100,000 orders/24 hours.
        self.placement_count.setdefault('SECOND', 0)
        self.placement_count.setdefault('HOUR', 0)
        counter = 0
        while True:
            self.placement_count['SECOND'] = max(0, self.placement_count['SECOND'] - 5)
            if counter > 120:
                counter = 0
                self.placement_count['HOUR'] = max(0, self.placement_count['HOUR'] - 70)
            else:
                counter += 1
            sleep(0.5)

    def verify_account_data(self):
        balance = self.get_balance()
        error = self.account.validate_balance(balance)
        self.notify('verify', update={'balance': error}, source='rest')

        if self.account.orders is not None:
            pairs = list(self.account.orders.keys())
            for pair in pairs:
                open_orders = self.get_open_orders(pair)
                error = self.account.validate_orders(open_orders)
                if pair in error:
                    self.notify('verify', update={'open_orders': [pair]}, source='rest')

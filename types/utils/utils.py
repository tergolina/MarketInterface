from datetime import datetime, timezone
from time import time
from dateutil.parser import parse


def pair_id_to_standard(exchange, pair_id):
    if exchange.lower() == 'poloniex':
        if pair_id in POLONIEX_PAIR_ID:
            poloniex_pair = POLONIEX_PAIR_ID[pair_id]
            b, a = poloniex_pair.split('_')
            return a + '/' + b
        else:
            return ''
    else:
        return ''


def pair_to_exchange(exchange, pair, method='default'):
    if exchange.lower() == 'binance':
        return pair.replace('STR', 'XLM').replace('/', '')
    elif exchange.lower() == 'poloniex':
        a, b = pair.split('/')
        return b + '_' + a
    elif exchange.lower() == 'bithumb':
        return pair.replace('/KRW', '')
    elif exchange.lower() == 'kraken':
        response = ''
        if method == 'default':
            if pair in ['BCH/EUR', 'BCH/USD']:
                response = pair.replace('/', '')
            else:
                for asset in pair.split('/'):
                    if asset in ['EUR', 'USD', 'CAD', 'GBP']:
                        response += 'Z' + asset
                    else:
                        response += 'X' + asset
        else:
            response = pair
        return response.replace('BTC', 'XBT')
    elif exchange.lower() == 'bitstamp':
        return pair.replace('/', '').lower()
    elif exchange.lower() == 'bitfinex':
        pair = pair.replace('USDT', 'UST')
        return pair.replace('/', '')
    elif exchange.lower() == 'hitbtc':
        if pair.split('/')[0] != 'XRP':
            pair = pair.replace('USDT', 'USD')
        return pair.replace('STR', 'XLM').replace('/', '')
    elif exchange.lower() == 'coinbase':
        return pair.replace('/', '-')
    else:
        return pair


def pair_to_standard(exchange, pair):
    if exchange.lower() == 'binance':
        standard_pair = pair
        if len(pair) >= 5:
            bases = ['BNB', 'BTC', 'ETH', 'USDT', 'PAX', 'XRP', 'USDC', 'TUSD', 'USDS']
            for base in bases:
                if pair[-len(base):] == base:
                    standard_pair = pair[:-len(base)] + '/' + base
                    break
        return standard_pair.replace('XLM', 'STR')
    elif exchange.lower() == 'poloniex':
        b, a = pair.split('_')
        return a + '/' + b
    elif exchange.lower() == 'bithumb':
        return pair + '/KRW'
    elif exchange.lower() == 'kraken':
        if len(pair) == 8:
            a = pair[1:4]
            b = pair[5:]
        else:
            a = pair[:3]
            b = pair[3:]
        return (a + '/' + b).replace('XBT', 'BTC')
    elif exchange.lower() == 'bitstamp':
        pair = pair.upper()
        bases = ['USD', 'EUR', 'BTC', 'GBP', 'JPY', 'CAD', 'ETH']
        for base in bases:
            if pair[-len(base):] == base:
                pair = pair[:-len(base)] + '/' + base
                break
        return pair
    elif exchange.lower() == 'bitfinex':
        if pair[0] == 't':
            pair = pair[1:]
            pair.replace('UST', 'USDT')
        if pair[-4:].upper() == 'USDT':
            a = pair[:-4]
            b = pair[-4:]
        else:
            a = pair[:-3]
            b = pair[-3:]
        return (a + '/' + b).upper()
    elif exchange.lower() == 'coinbase':
        return pair.replace('-', '/')
    elif exchange.lower() == 'exmo':
        return pair.replace('_', '/')
    elif exchange.lower() == 'bittrex':
        a, b = pair.split('-')
        return b + '/' + a
    elif exchange.lower() == 'cex':
        return pair.replace(':', '/')
    elif exchange.lower() == 'hitbtc':
        if 'USDT' == pair[-4:]:
            a = pair[:-4]
            b = pair[-4:]
        else:
            a = pair[:-3]
            b = pair[-3:]
            if b == 'USD':
                b = 'USDT'
        return (a + '/' + b).replace('XLM', 'STR')
    else:
        return pair


def order_to_standard(exchange, raw_order, pair='', source='websocket', type='balance'):
    if exchange.lower() == 'hitbtc':
        order = {'timestamp': datetime.strptime(raw_order['updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp(),
                 'exchange': exchange.lower(),
                 'id': raw_order['clientOrderId'],
                 'pair': pair_to_standard('hitbtc', raw_order['symbol']),
                 'side': raw_order['side'],
                 'price': float(raw_order['price']),
                 'quantity': float(raw_order['quantity']) - float(raw_order['cumQuantity']),
                 'volume': float(raw_order['price']) * (float(raw_order['quantity']) - float(raw_order['cumQuantity'])),
                 'type': 'balance'}
    elif exchange.lower() == 'poloniex':
        if 'orderNumber' in raw_order:
            leverageble_pairs = ['STR/BTC', 'XRP/BTC', 'LTC/BTC', 'XMR/BTC', 'ETH/BTC', 'DASH/BTC', 'MAID/BTC', 'DOGE/BTC', 'FCT/BTC', 'CLAM/BTC']
            type = 'margin' if pair in leverageble_pairs else 'balance'
            order = {'exchange': exchange.lower(),
                     'id': int(raw_order['orderNumber']),
                     'pair': pair,
                     'side': raw_order['type'],
                     'price': float(raw_order['rate']),
                     'quantity': float(raw_order['amount']),
                     'volume': float(raw_order['total']),
                     'type': type}
        else:
            pair = pair_id_to_standard('poloniex', raw_order[1])
            order = {'exchange': exchange.lower(),
                     'id' : raw_order[2],
                     'pair': pair,
                     'side' : 'buy' if raw_order[3] == 1 else 'sell',
                     'price' : float(raw_order[4]),
                     'quantity': float(raw_order[5]),
                     'volume': float(raw_order[4])*float(raw_order[5]),
                     'type': type}
    elif exchange.lower() == 'kraken':
        order = {'exchange': exchange,
                 'id': raw_order['id'],
                 'pair': pair_to_standard(exchange, raw_order['descr']['pair']),
                 'side': raw_order['descr']['type'],
                 'price': float(raw_order['descr']['price']),
                 'quantity': float(raw_order['vol']) - float(raw_order['vol_exec']),
                 'volume': float(raw_order['descr']['price']) * (float(raw_order['vol']) - float(raw_order['vol_exec'])),
                 'type': 'margin'}
    elif exchange.lower() == 'bitmex':
        order = {'exchange': exchange.lower(),
                 'id': raw_order['orderID'],
                 'pair': raw_order['symbol'],
                 'side': raw_order['side'].lower(),
                 'price': float(raw_order['price']),
                 'quantity': float(raw_order['orderQty']),
                 'volume': float(raw_order['orderQty']/raw_order['price']),
                 'type': 'margin',
                 'tag': raw_order['clOrdID']}
    elif exchange.lower() == 'binance':
        if source == 'websocket':
            if float(raw_order['L']) > 0:
                price = float(raw_order['L'])
            else:
                price = float(raw_order['p'])
            order = {'exchange': exchange.lower(),
                     'id': raw_order['i'],
                     'pair': pair_to_standard(exchange, raw_order['s']),
                     'side': raw_order['S'].lower(),
                     'price': price,
                     'quantity': float(raw_order['q']) - float(raw_order['z']),
                     'volume': price * (float(raw_order['q']) - float(raw_order['z'])),
                     'type': 'balance'}
        else:
            order = {'exchange': exchange.lower(),
                     'id': raw_order['orderId'],
                     'pair': pair_to_standard(exchange, raw_order['symbol']),
                     'side': raw_order['side'].lower(),
                     'price': float(raw_order['price']),
                     'quantity': float(raw_order['origQty']) - float(raw_order['executedQty']),
                     'volume': (float(raw_order['origQty']) - float(raw_order['executedQty'])) * float(raw_order['price']),
                     'type': 'balance'}
    elif exchange.lower() == 'bitfinex':
        if isinstance(raw_order, list):
            order = {'exchange': exchange.lower(),
                     'id': raw_order[0],
                     'pair': pair_to_standard(exchange, raw_order[3]),
                     'side': 'buy' if raw_order[6] > 0 else 'sell',
                     'price': raw_order[16],
                     'quantity': abs(raw_order[6]),
                     'volume': raw_order[16] * abs(raw_order[6]),
                     'type': 'balance' if 'exchange' in (raw_order[8]).lower() else 'margin',
                     'fills': len(raw_order[13])}
        else:
            order = {'exchange': exchange.lower(),
                     'id': raw_order['id'],
                     'pair': pair_to_standard(exchange, raw_order['symbol']),
                     'side': raw_order['side'],
                     'price': float(raw_order['price']),
                     'quantity': float(raw_order['remaining_amount']),
                     'volume': float(raw_order['price']) * float(raw_order['remaining_amount']),
                     'type': 'balance' if ('exchange' in raw_order['type']) else 'margin'}
    else:
        order = raw_order
    return order


def trade_to_standard(exchange, raw_trade, pair='', source='websocket'):
    trade = raw_trade
    if exchange.lower() == 'hitbtc':
        if source == 'websocket':
            trade = {'timestamp': datetime.strptime(raw_trade['updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp(),
                     'exchange': exchange.lower(),
                     'id': raw_trade['tradeId'],
                     'pair': pair_to_standard('hitbtc', raw_trade['symbol']),
                     'side': raw_trade['side'],
                     'price': float(raw_trade['tradePrice']),
                     'quantity': float(raw_trade['tradeQuantity']),
                     'volume': float(raw_trade['tradePrice']) * float(raw_trade['tradeQuantity'])}
        elif source == 'rest':
            trade = {'timestamp': datetime.strptime(raw_trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp(),
                     'exchange': exchange.lower(),
                     'id': raw_trade['id'],
                     'pair': pair,
                     'side': raw_trade['side'],
                     'price': float(raw_trade['price']),
                     'quantity': float(raw_trade['quantity']),
                     'volume': float(raw_trade['price']) * float(raw_trade['quantity'])}
    elif exchange.lower() == 'binance':
        if source == 'websocket':
            trade = {'timestamp': float(raw_trade['T']/1000),
                     'exchange': exchange.lower(),
                     'id': raw_trade['a'],
                     'pair': pair_to_standard(exchange, raw_trade['s']),
                     'side': 'sell' if raw_trade['m'] else 'buy',
                     'price': float(raw_trade['p']),
                     'quantity': float(raw_trade['q']),
                     'volume': float(raw_trade['p']) * float(raw_trade['q'])}
        elif source == 'rest':
            trade = {'timestamp': float(raw_trade['T']/1000),
                     'exchange': exchange.lower(),
                     'id': raw_trade['a'],
                     'pair': pair,
                     'side': 'sell' if raw_trade['m'] else 'buy',
                     'price': float(raw_trade['p']),
                     'quantity': float(raw_trade['q']),
                     'volume': float(raw_trade['p']) * float(raw_trade['q'])}
    elif exchange.lower() == 'poloniex':
        trade = {'timestamp': datetime.strptime(raw_trade['date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp(),
                 'exchange': exchange.lower(),
                 'pair': pair,
                 'side': raw_trade['type'],
                 'price': float(raw_trade['rate']),
                 'quantity': float(raw_trade['amount']),
                 'volume': float(raw_trade['total'])}
    elif exchange.lower() == 'bitmex':
        trade = {'timestamp': datetime.strptime(raw_trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp(),
                 'exchange': exchange.lower(),
                 'id': raw_trade['trdMatchID'],
                 'pair': raw_trade['symbol'],
                 'side': raw_trade['side'].lower(),
                 'price': float(raw_trade['price']),
                 'quantity': float(raw_trade['size']),
                 'volume': float(raw_trade['size'])}
    elif exchange.lower() == 'bitfinex':
        if len(raw_trade) == 4:
            trade = {'timestamp': raw_trade[1]/1000,
                     'exchange': exchange.lower(),
                     'id': raw_trade[0],
                     'pair': pair,
                     'side': 'buy' if raw_trade[2] >= 0 else 'sell',
                     'price': raw_trade[3],
                     'quantity': abs(raw_trade[2]),
                     'volume': raw_trade[3] * abs(raw_trade[2])}
        else:
            trade = {'timestamp': raw_trade[2]/1000,
                     'exchange': exchange.lower(),
                     'id': raw_trade[0],
                     'pair': pair_to_standard(exchange, raw_trade[1]),
                     'side': 'buy' if raw_trade[4] >= 0 else 'sell',
                     'price': raw_trade[5],
                     'quantity': abs(raw_trade[4]),
                     'volume': raw_trade[5] * abs(raw_trade[4])}
    elif exchange.lower() == 'kraken':
            trade = {'timestamp': float(raw_trade[2]),
                     'exchange': exchange.lower(),
                     'pair': pair,
                     'side': 'buy' if raw_trade[3] == 'b' else 'sell',
                     'price': float(raw_trade[0]),
                     'quantity': float(raw_trade[1]),
                     'volume': float(raw_trade[0]) * float(raw_trade[1])}
    return trade


def type_to_exchange(exchange, type):
    if exchange.lower() == 'binance':
        if type == 'maker':
            return 'LIMIT_MAKER'
        elif type == 'market':
            return 'MARKET'
        else:
            return 'LIMIT'
    elif exchange.lower() == 'bitfinex':
        if type == 'maker':
            return 'LIMIT'
        elif type == 'sniper':
            return 'IOC'
        elif type == 'market':
            return 'MARKET'
        else:
            return 'LIMIT'
    elif exchange.lower() == 'kraken':
        if type == 'sniper':
            return 'limit'
        elif type == 'maker':
            return 'limit'
        else:
            return type
    else:
        return type


def round_price(exchange, price, pair=''):
    if exchange.lower() == 'kraken':
        if ('/BTC' in pair):
            price = int(price * 100000) / 100000
        elif ('ETH' in pair) or ('BCH' in pair) or ('XMR' in pair):
            price = int(price * 100) / 100
        elif 'XRP' in pair:
            price = int(price * 100000) / 100000
        else:
            price = int(price * 10) / 10
    elif exchange.lower() == 'binance':
        if pair in ['ETH/USDT', 'BTC/USDT', 'BCH/USDT', 'LTC/USDT']:
            return int(price * 100) / 100
        elif pair in ['NEO/USDT']:
            return int(price * 1000) / 1000
        elif pair in ['EOS/USDT', 'BNB/USDT', 'ETC/USDT', 'IOTA/USDT']:
            return int(price * 10000) / 10000
        elif pair in ['XRP/USDT', 'ADA/USDT', 'TRX/USDT', 'STR/USDT']:
            return int(price * 100000) / 100000
        elif pair in ['ETH/BTC', 'BCH/BTC', 'LTC/BTC']:
            return int(price * 1000000) / 1000000
        elif pair in ['BNB/BTC', 'EOS/BTC']:
            return int(price * 10000000) / 10000000
        elif pair in ['XRP/BTC']:
            return int(price * 100000000) / 100000000
    return price


def round_quantity(exchange, quantity, pair=''):
    if exchange.lower() == 'kraken':
        return int(quantity * 1000) / 1000
    elif exchange.lower() == 'binance':
        if pair in ['ETH/USDT', 'BTC/USDT']:
            return int(quantity * 1000000) / 1000000
        elif pair in ['LTC/USDT', 'BCH/USDT']:
            return int(quantity * 100000) / 100000
        elif pair in ['NEO/USDT', 'ETH/BTC', 'BCH/BTC']:
            return int(price * 1000) / 1000
        elif pair in ['EOS/USDT', 'BNB/USDT', 'ETC/USDT', 'IOTA/USDT', 'BNB/BTC', 'EOS/BTC', 'LTC/BTC']:
            return int(quantity * 100) / 100
        elif pair in ['XRP/USDT', 'ADA/USDT', 'TRX/USDT', 'STR/USDT']:
            return int(quantity * 10) / 10
        elif pair in ['XRP/BTC']:
            return int(quantity)
        else:
            return quantity


def order_list_to_standard(order_list):
    new_orders = {}
    for order in order_list:
        if order != {}:
            side = 'bid' if order['side'] == 'buy' else 'ask'
            pair = order['pair']
            if pair not in new_orders:
                new_orders[pair] = {'bid': [], 'ask': []}
            new_orders[pair][side] += [order]
    return new_orders


def date_print(text):
    print('{} - {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), text))


def nonce():
    return int(time()*1000)


def error_to_standard(exchange, message):
    if exchange == 'binance':
        if 'error' in message and 'message' in message['error']:
            message = message['error']['message']
            if 'msg' in message:
                message = str(message['msg'])
    return str(message)

POLONIEX_PAIR_ID = {7: 'BTC_BCN',
                    12: 'BTC_BTCD',
                    13: 'BTC_BTM',
                    14: 'BTC_BTS',
                    15: 'BTC_BURST',
                    20: 'BTC_CLAM',
                    25: 'BTC_DGB',
                    27: 'BTC_DOGE',
                    24: 'BTC_DASH',
                    28: 'BTC_EMC2',
                    38: 'BTC_GAME',
                    43: 'BTC_HUC',
                    50: 'BTC_LTC',
                    51: 'BTC_MAID',
                    58: 'BTC_OMNI',
                    61: 'BTC_NAV',
                    63: 'BTC_NEOS',
                    64: 'BTC_NMC',
                    69: 'BTC_NXT',
                    74: 'BTC_POT',
                    75: 'BTC_PPC',
                    89: 'BTC_STR',
                    92: 'BTC_SYS',
                    97: 'BTC_VIA',
                    99: 'BTC_VRC',
                    100: 'BTC_VTC',
                    104: 'BTC_XBC',
                    108: 'BTC_XCP',
                    114: 'BTC_XMR',
                    116: 'BTC_XPM',
                    117: 'BTC_XRP',
                    112: 'BTC_XEM',
                    40: 'BTC_GRC',
                    148: 'BTC_ETH',
                    150: 'BTC_SC',
                    153: 'BTC_EXP',
                    155: 'BTC_FCT',
                    160: 'BTC_AMP',
                    162: 'BTC_DCR',
                    163: 'BTC_LSK',
                    167: 'BTC_LBC',
                    168: 'BTC_STEEM',
                    170: 'BTC_SBD',
                    171: 'BTC_ETC',
                    174: 'BTC_REP',
                    177: 'BTC_ARDR',
                    178: 'BTC_ZEC',
                    182: 'BTC_STRAT',
                    184: 'BTC_PASC',
                    185: 'BTC_GNT',
                    187: 'BTC_GNO',
                    189: 'BTC_BCH',
                    192: 'BTC_ZRX',
                    194: 'BTC_CVC',
                    196: 'BTC_OMG',
                    198: 'BTC_GAS',
                    200: 'BTC_STORJ',
                    201: 'BTC_EOS',
                    204: 'BTC_SNT',
                    207: 'BTC_KNC',
                    210: 'BTC_BAT',
                    213: 'BTC_LOOM',
                    221: 'BTC_QTUM',
                    121: 'USDT_BTC',
                    216: 'USDT_DOGE',
                    122: 'USDT_DASH',
                    123: 'USDT_LTC',
                    124: 'USDT_NXT',
                    125: 'USDT_STR',
                    126: 'USDT_XMR',
                    127: 'USDT_XRP',
                    149: 'USDT_ETH',
                    219: 'USDT_SC',
                    218: 'USDT_LSK',
                    173: 'USDT_ETC',
                    175: 'USDT_REP',
                    180: 'USDT_ZEC',
                    217: 'USDT_GNT',
                    191: 'USDT_BCH',
                    220: 'USDT_ZRX',
                    203: 'USDT_EOS',
                    206: 'USDT_SNT',
                    209: 'USDT_KNC',
                    212: 'USDT_BAT',
                    215: 'USDT_LOOM',
                    223: 'USDT_QTUM',
                    129: 'XMR_BCN',
                    131: 'XMR_BTCD',
                    132: 'XMR_DASH',
                    137: 'XMR_LTC',
                    138: 'XMR_MAID',
                    140: 'XMR_NXT',
                    181: 'XMR_ZEC',
                    166: 'ETH_LSK',
                    169: 'ETH_STEEM',
                    172: 'ETH_ETC',
                    176: 'ETH_REP',
                    179: 'ETH_ZEC',
                    186: 'ETH_GNT',
                    188: 'ETH_GNO',
                    190: 'ETH_BCH',
                    193: 'ETH_ZRX',
                    195: 'ETH_CVC',
                    197: 'ETH_OMG',
                    199: 'ETH_GAS',
                    202: 'ETH_EOS',
                    205: 'ETH_SNT',
                    208: 'ETH_KNC',
                    211: 'ETH_BAT',
                    214: 'ETH_LOOM',
                    222: 'ETH_QTUM'}
POLONIEX_CURRENCY_ID = {1: '1CR',
                        2: 'ABY',
                        3: 'AC',
                        4: 'ACH',
                        5: 'ADN',
                        6: 'AEON',
                        7: 'AERO',
                        8: 'AIR',
                        9: 'APH',
                        10: 'AUR',
                        11: 'AXIS',
                        12: 'BALLS',
                        13: 'BANK',
                        14: 'BBL',
                        15: 'BBR',
                        16: 'BCC',
                        17: 'BCN',
                        18: 'BDC',
                        19: 'BDG',
                        20: 'BELA',
                        21: 'BITS',
                        22: 'BLK',
                        23: 'BLOCK',
                        24: 'BLU',
                        25: 'BNS',
                        26: 'BONES',
                        27: 'BOST',
                        28: 'BTC',
                        29: 'BTCD',
                        30: 'BTCS',
                        31: 'BTM',
                        32: 'BTS',
                        33: 'BURN',
                        34: 'BURST',
                        35: 'C2',
                        36: 'CACH',
                        37: 'CAI',
                        38: 'CC',
                        39: 'CCN',
                        40: 'CGA',
                        41: 'CHA',
                        42: 'CINNI',
                        43: 'CLAM',
                        44: 'CNL',
                        45: 'CNMT',
                        46: 'CNOTE',
                        47: 'COMM',
                        48: 'CON',
                        49: 'CORG',
                        50: 'CRYPT',
                        51: 'CURE',
                        52: 'CYC',
                        53: 'DGB',
                        54: 'DICE',
                        55: 'DIEM',
                        56: 'DIME',
                        57: 'DIS',
                        58: 'DNS',
                        59: 'DOGE',
                        60: 'DASH',
                        61: 'DRKC',
                        62: 'DRM',
                        63: 'DSH',
                        64: 'DVK',
                        65: 'EAC',
                        66: 'EBT',
                        67: 'ECC',
                        68: 'EFL',
                        69: 'EMC2',
                        70: 'EMO',
                        71: 'ENC',
                        72: 'eTOK',
                        73: 'EXE',
                        74: 'FAC',
                        75: 'FCN',
                        76: 'FIBRE',
                        77: 'FLAP',
                        78: 'FLDC',
                        79: 'FLT',
                        80: 'FOX',
                        81: 'FRAC',
                        82: 'FRK',
                        83: 'FRQ',
                        84: 'FVZ',
                        85: 'FZ',
                        86: 'FZN',
                        87: 'GAP',
                        88: 'GDN',
                        89: 'GEMZ',
                        90: 'GEO',
                        91: 'GIAR',
                        92: 'GLB',
                        93: 'GAME',
                        94: 'GML',
                        95: 'GNS',
                        96: 'GOLD',
                        97: 'GPC',
                        98: 'GPUC',
                        99: 'GRCX',
                        100: 'GRS',
                        101: 'GUE',
                        102: 'H2O',
                        103: 'HIRO',
                        104: 'HOT',
                        105: 'HUC',
                        106: 'HVC',
                        107: 'HYP',
                        108: 'HZ',
                        109: 'IFC',
                        110: 'ITC',
                        111: 'IXC',
                        112: 'JLH',
                        113: 'JPC',
                        114: 'JUG',
                        115: 'KDC',
                        116: 'KEY',
                        117: 'LC',
                        118: 'LCL',
                        119: 'LEAF',
                        120: 'LGC',
                        121: 'LOL',
                        122: 'LOVE',
                        123: 'LQD',
                        124: 'LTBC',
                        125: 'LTC',
                        126: 'LTCX',
                        127: 'MAID',
                        128: 'MAST',
                        129: 'MAX',
                        130: 'MCN',
                        131: 'MEC',
                        132: 'METH',
                        133: 'MIL',
                        134: 'MIN',
                        135: 'MINT',
                        136: 'MMC',
                        137: 'MMNXT',
                        138: 'MMXIV',
                        139: 'MNTA',
                        140: 'MON',
                        141: 'MRC',
                        142: 'MRS',
                        143: 'OMNI',
                        144: 'MTS',
                        145: 'MUN',
                        146: 'MYR',
                        147: 'MZC',
                        148: 'N5X',
                        149: 'NAS',
                        150: 'NAUT',
                        151: 'NAV',
                        152: 'NBT',
                        153: 'NEOS',
                        154: 'NL',
                        155: 'NMC',
                        156: 'NOBL',
                        157: 'NOTE',
                        158: 'NOXT',
                        159: 'NRS',
                        160: 'NSR',
                        161: 'NTX',
                        162: 'NXT',
                        163: 'NXTI',
                        164: 'OPAL',
                        165: 'PAND',
                        166: 'PAWN',
                        167: 'PIGGY',
                        168: 'PINK',
                        169: 'PLX',
                        170: 'PMC',
                        171: 'POT',
                        172: 'PPC',
                        173: 'PRC',
                        174: 'PRT',
                        175: 'PTS',
                        176: 'Q2C',
                        177: 'QBK',
                        178: 'QCN',
                        179: 'QORA',
                        180: 'QTL',
                        181: 'RBY',
                        182: 'RDD',
                        183: 'RIC',
                        184: 'RZR',
                        185: 'SDC',
                        186: 'SHIBE',
                        187: 'SHOPX',
                        188: 'SILK',
                        189: 'SJCX',
                        190: 'SLR',
                        191: 'SMC',
                        192: 'SOC',
                        193: 'SPA',
                        194: 'SQL',
                        195: 'SRCC',
                        196: 'SRG',
                        197: 'SSD',
                        198: 'STR',
                        199: 'SUM',
                        200: 'SUN',
                        201: 'SWARM',
                        202: 'SXC',
                        203: 'SYNC',
                        204: 'SYS',
                        205: 'TAC',
                        206: 'TOR',
                        207: 'TRUST',
                        208: 'TWE',
                        209: 'UIS',
                        210: 'ULTC',
                        211: 'UNITY',
                        212: 'URO',
                        213: 'USDE',
                        214: 'USDT',
                        215: 'UTC',
                        216: 'UTIL',
                        217: 'UVC',
                        218: 'VIA',
                        219: 'VOOT',
                        220: 'VRC',
                        221: 'VTC',
                        222: 'WC',
                        223: 'WDC',
                        224: 'WIKI',
                        225: 'WOLF',
                        226: 'X13',
                        227: 'XAI',
                        228: 'XAP',
                        229: 'XBC',
                        230: 'XC',
                        231: 'XCH',
                        232: 'XCN',
                        233: 'XCP',
                        234: 'XCR',
                        235: 'XDN',
                        236: 'XDP',
                        237: 'XHC',
                        238: 'XLB',
                        239: 'XMG',
                        240: 'XMR',
                        241: 'XPB',
                        242: 'XPM',
                        243: 'XRP',
                        244: 'XSI',
                        245: 'XST',
                        246: 'XSV',
                        247: 'XUSD',
                        248: 'XXC',
                        249: 'YACC',
                        250: 'YANG',
                        251: 'YC',
                        252: 'YIN',
                        253: 'XVC',
                        254: 'FLO',
                        256: 'XEM',
                        258: 'ARCH',
                        260: 'HUGE',
                        261: 'GRC',
                        263: 'IOC',
                        265: 'INDEX',
                        267: 'ETH',
                        268: 'SC',
                        269: 'BCY',
                        270: 'EXP',
                        271: 'FCT',
                        272: 'BITUSD',
                        273: 'BITCNY',
                        274: 'RADS',
                        275: 'AMP',
                        276: 'VOX',
                        277: 'DCR',
                        278: 'LSK',
                        279: 'DAO',
                        280: 'LBC',
                        281: 'STEEM',
                        282: 'SBD',
                        283: 'ETC',
                        284: 'REP',
                        285: 'ARDR',
                        286: 'ZEC',
                        287: 'STRAT',
                        288: 'NXC',
                        289: 'PASC',
                        290: 'GNT',
                        291: 'GNO',
                        292: 'BCH',
                        293: 'ZRX',
                        294: 'CVC',
                        295: 'OMG',
                        296: 'GAS',
                        297: 'STORJ',
                        298: 'EOS',
                        300: 'SNT',
                        301: 'KNC',
                        302: 'BAT',
                        303: 'LOOM',
                        304: 'QTUM'}


class DummyLogger:
    def __init__(self, name):
        pass

    def log(self, tag, data, silenced=False, print_only=False):
        if print_only:
            print (datetime.now(), '- [', tag, ']', data)

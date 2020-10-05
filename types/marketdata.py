from queue import Queue


class MarketData:
    def __init__(self):
        self.last = {}
        self.bid = {}
        self.ask = {}
        self.bid_quantity = {}
        self.ask_quantity = {}
        self.book = {}
        self.book_queue = {}
        # Notify control -------------------------------------------------------
        self.last_notified_buy = {}
        self.last_notified_sell = {}

    def get_data(self):
        """ Retorna os dados de ticker do objeto marketdata

            Returns:
                (dict): Dicionario contendo os precos de bid, ask e last de
                    todos pares
        """
        d = {'bid': self.bid,
             'ask': self.ask,
             'bid_quantity': self.bid_quantity,
             'ask_quantity': self.ask_quantity,
             'last_buy': self.last_notified_buy,
             'last_sell': self.last_notified_sell,
             'last': self.last,
             'book': self.book}
        return d

    def set_market_data(self, data):
        if 'bid' in data:
            self.bid = data['bid']
        if 'ask' in data:
            self.ask = data['ask']
        if 'last' in data:
            self.last = data['last']

    def update_market_data(self, pair, data, side=None, tolerance=0):
        """ Atualiza os dados basicos de ticker

            Args:
                pair (str): Par no formato padrao Asimov
                data (dict): Dicionario contendo bid, ask ou last
            Returns:
                (bool): Se houve mudanca nos precos
        """
        change = False
        previous = 0
        if 'last' in data:
            if pair in self.last:
                previous = self.last[pair]
            self.last[pair] = data['last']
            if side == 'buy':
                if abs((self.last_notified_buy.get(pair, 0) / self.last[pair]) - 1) > tolerance:
                    self.last_notified_buy[pair] = self.last[pair]
                    change = True
            elif side == 'sell':
                if abs((self.last_notified_sell.get(pair, 0) / self.last[pair]) - 1) > tolerance:
                    self.last_notified_sell[pair] = self.last[pair]
                    change = True
            else:
                change = previous != self.last[pair]

        if 'bid' in data:
            if pair in self.bid:
                previous = self.bid[pair]
            self.bid[pair] = data['bid']
            change = change or (previous != self.bid[pair])
        if 'ask' in data:
            if pair in self.ask:
                previous = self.ask[pair]
            self.ask[pair] = data['ask']
            change = change or (previous != self.ask[pair])
        return change

    def queue_entry(self, entry):
        """ Coloca na fila uma entrada nova de book

            Args:
                entry (dict): Dicionario a ser tratado para dentro do book. Pode
                    ser um snapshot ou uma atualizacao
        """
        if 'pair' in entry:
            pair = entry['pair']
            if pair not in self.book_queue:
                self.book_queue[pair] = Queue()
            self.book_queue[pair].put(entry)

    def insert_entry(self, entry):
        """ Insere uma entrada nova no book

            Args:
                entry (dict): Dicionario com as chaves 'pair', 'side', 'price' e
                    'quantity' a serem atualizadas no book. No caso da quantidade
                    ser 0, deleta a linha do book
            Returns:
                (bool): Se houve ou nao mudanca no preco do topo do book
        """
        change = False
        if 'pair' in entry:
            pair = entry['pair']
            book_side = entry['side']
            price = entry['price']
            quantity = entry['quantity']
            if pair not in self.book:
                self.book[pair] = {'bid': {}, 'ask': {}}
            if quantity == 0:
                if price in self.book[pair][book_side]:
                    del self.book[pair][book_side][price]
            else:
                self.book[pair][book_side][price] = quantity
            change = self.update_book_top(pair, book_side)
        return change

    def set_book(self, pair, book_side, raw_book):
        """ Sobrescreve o book com um snapshot

            Args:
                pair (str): Par no formato padrao Asimov
                book_side (str): Lado do book ('bid' ou 'ask')
                raw_book (dict): Book no formato {P1: Q1, P2: Q2, P3: Q3 ...}
                    onde Pn e uma string identificando o preco da linha e Qn e a
                    quantidade total em float na linha.

            Returns:
                (bool): Se houve ou nao mudanca no preco do topo do book
        """
        if pair not in self.book:
            self.book[pair] = {'bid': {}, 'ask': {}}
        self.book[pair][book_side] = {price: float(raw_book[price]) for price in raw_book}
        return self.update_book_top(pair, book_side)

    def update_book_top(self, pair, book_side):
        """ Atualiza os precos do topo do book

            Args:
                pair (str): Par no formato padrao Asimov
                book_side (str): Lado do book ('bid' ou 'ask')

            Returns:
                (bool): Se houve ou nao mudanca no preco do topo do book
        """
        previous = 0
        change = False
        if (pair in self.book) and (book_side in self.book[pair]) and (self.book[pair][book_side] != {}):
            if book_side == 'bid':
                if pair in self.bid:
                    previous = self.bid[pair]
                raw_bid = max(self.book[pair]['bid'])
                self.bid[pair] = float(raw_bid)
                self.bid_quantity[pair] = self.book[pair]['bid'][raw_bid]
                change = previous != self.bid[pair]
            elif book_side == 'ask':
                if pair in self.ask:
                    previous = self.ask[pair]
                raw_ask = min(self.book[pair]['ask'])
                self.ask[pair] = float(raw_ask)
                self.ask_quantity[pair] = self.book[pair]['ask'][raw_ask]
                change = change or (previous != self.ask[pair])
        return change

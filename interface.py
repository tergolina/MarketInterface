def Interface(exchange, subscriber=None, blueprint=None, logger=None, quick=False, hot=False, filter=None, book_depth=False, no_poll=False):
    """ Cria uma interface

        Args:
            exchange (str): Exchange da interface
            subscriber (callable): Funcao handler das mensagens enviadas pela interface
            blueprint (dict): Esquema que descreve o que deve conter na interface
            logger (asimov.Logger): Objeto logger Asimov
        Returns:
            (Exchange): Interface
    """
    blueprint = {'marketdata': []} if blueprint is None else blueprint
    if exchange.lower() == 'binance':
        from .binance import Binance
        return Binance(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    elif exchange.lower() == 'poloniex':
        from .poloniex import Poloniex
        return Poloniex(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    elif exchange.lower() == 'hitbtc':
        from .hitbtc import Hitbtc
        return Hitbtc(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    elif exchange.lower() == 'bitmex':
        from .bitmex import Bitmex
        return Bitmex(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    elif exchange.lower() == 'bitfinex':
        from .bitfinex import Bitfinex
        return Bitfinex(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    elif exchange.lower() == 'kraken':
        from .kraken import Kraken
        return Kraken(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)
    else:
        from .types.exchange import Exchange
        return Exchange(subscriber=subscriber, blueprint=blueprint, filter=filter, logger=logger, quick=quick, hot=hot, book_depth=book_depth, no_poll=no_poll)

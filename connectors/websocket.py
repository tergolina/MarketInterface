from time import sleep, time
from datetime import datetime
from threading import Thread, Event, Lock
from websocket import WebSocketApp
import json


class WebSocket:
    """ Classe de WebSocket Asimov """
    def __init__(self, url, handler=None, subs=[], header=None, hot_handler=None):
        """ Inicializacao da classe

            Args:
                url (str/callable): Url ou funcao que retorne uma url que sera
                    usada na criacao do WebSocket
                handler (callable): Funcao para qual as mensagens serao repassadas
                subs (list): Lista com payloads ou funcoes que retornem payloads
                    que serao enviados pelo WebSocket logo apos a inicializacao
        """
        self.url = url
        self.handler = handler
        self.hot_handler = hot_handler
        self.header = header
        # Control --------------------------------------------------------------
        self.on_handshake = Event()
        self.on_handshake.set()
        self.flood_lock = Lock()
        self.started = False
        self.keep_alive = True
        self.error_count = 0
        self.id = -1
        self.score = -1
        # Websocket ------------------------------------------------------------
        self.websocket = None
        self.th_websocket = None
        self.subscriptions = subs
        # Check ----------------------------------------------------------------
        self.last_heartbeat = 0
        self.th_heartbeat = Thread(target=self.__heartbeat_check, args=[10])
        self.th_heartbeat.start()
        self.th_keep_alive = Thread(target=self.__keep_alive, args=[10])
        self.th_keep_alive.start()

    def __initialize_websocket(self):
        if self.keep_alive:
            if callable(self.url):
                url = self.url()
            else:
                url = self.url
            print(datetime.now(), '- [ WebSocket ] Initializing websocket. Subs:', self.subscriptions, '| Url:', url)
            if self.header == None:
                self.websocket = WebSocketApp(url,
                                              on_open=self.__on_open,
                                              on_message=self.__on_message,
                                              on_error=self.__on_error,
                                              on_close=self.__on_close)
            else:
                self.websocket = WebSocketApp(url,
                                              header=self.header,
                                              on_open=self.__on_open,
                                              on_message=self.__on_message,
                                              on_error=self.__on_error,
                                              on_close=self.__on_close)
            self.th_websocket = Thread(target=self.websocket.run_forever)
            # self.th_websocket.daemon = True
            self.th_websocket.start()

    def __on_open(self):
        self.on_handshake.clear()
        self.subscribe()

    def __on_message(self, message):
        self.last_heartbeat = time()
        if self.handler:
            self.handler(message)
        if self.hot_handler:
            self.hot_handler(message, self.id)

    def __on_error(self, error):
        print(datetime.now(), '- [ WebSocket ] ERROR:', error)
        self.handler(json.dumps({'error': error}))
        if '429' in error:
            self.flood_lock.acquire()
            sleep(200)
            self.flood_lock.release()

    def __on_close(self):
        print(datetime.now(), '- [ WebSocket ] CLOSED!')
        self.on_handshake.set()
        self.handler(json.dumps({'error': 'closed'}))

    def __keep_alive(self, _t):
        while True:
            self.on_handshake.wait()
            self.on_handshake.clear()
            self.flood_lock.acquire()
            self.flood_lock.release()
            self.__initialize_websocket()
            sleep(_t)

    def __heartbeat_check(self, _t):
        while True:
            sleep(_t)
            if (time() - self.last_heartbeat) > 100:
                if (self.th_websocket is None) or (not self.th_websocket.isAlive()):
                    self.on_handshake.set()
            if self.error_count >= 3:
                self.reset()
                self.on_handshake.set()
                self.error_count = 0

    def send(self, data):
        """ Envia um payload pelo WebSocket

            Args:
                data (json): Payload que sera enviado
        """
        message = json.dumps(data)
        if (self.th_websocket is not None) and (self.websocket is not None) and self.th_websocket.isAlive():
            try:
                self.websocket.send(message)
            except Exception as e:
                print(datetime.now(), '- [ WebSocket ] ERROR SENDING MESSAGE:', str(e))
                self.error_count += 1

    def subscribe(self):
        for sub in self.subscriptions:
            sleep(5)
            if callable(sub):
                self.send(sub())
            else:
                self.send(sub)
        self.started = True

    def close(self):
        self.keep_alive = False
        if (self.th_websocket is not None) and (self.websocket is not None):
            print(datetime.now(), '- [ WebSocket ] CLOSING...')
            self.websocket.close()

    def open(self):
        self.keep_alive = True

    def reset(self):
        print(datetime.now(), '- [ WebSocket ] RESETING...')
        if (self.th_websocket is not None) and (self.websocket is not None):
            self.websocket.close()

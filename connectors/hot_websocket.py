from time import sleep
from asimov.interface.connectors.websocket import WebSocket
from asimov.interface.types.utils.utils import *
from threading import Lock
import json
import gc


MAX_WS = 10
MAX_SIMULTANEOUS_WS = 4
MAX_WARMUP_WAIT = 4 # warmup every MAX_WARMUP_WAIT hours

class HotWebSocket:
    """ Selects the N fastest websockets and its signals"""

    def __init__(self, url, handler, subs=[]):
        """ Class initialization

            Args:
                url (str/callable): Url ou funcao que retorne uma url que sera
                    usada na criacao do WebSocket
                handler (callable): Funcao para qual as mensagens serao repassadas
                subs (list): Lista com payloads ou funcoes que retornem payloads
                    que serao enviados pelo WebSocket logo apos a inicializacao
        """
        self.url = url
        self.handler = handler
        self.subs = subs
        self.websockets = []
        self.ws_created = MAX_SIMULTANEOUS_WS
        self.last_timestamp = 0
        self.last_cleanup = time()  # last time of websocket cleanup
        self.last_warmup = time()
        self.name = "bitmex"
        self.hot_id = 0
        self.cold_id = -1
        self.check_lock = Lock()
        for i in range(0, MAX_SIMULTANEOUS_WS):
            self.websockets.append(WebSocket(self.url, hot_handler=self.dispatcher_handler, subs=subs))
            self.websockets[-1].id = i
            sleep(1)

    def dispatcher_handler(self, message, ws_id):
        json_response = json.loads(message)

        if ('data' in json_response) and ("trade" in json_response["table"]):
            # send always the fastest message

            trade = trade_to_standard(self.name, json_response["data"][-1])

            if trade["timestamp"] > self.last_timestamp:
                self.last_timestamp = trade["timestamp"]
                self.websockets[ws_id].score += 1
                self.handler(message)
        else:
            # if is not a trade, send only from the hottest websocket
            if ws_id == self.hot_id:
                self.handler(message)

        # close all slow websockets
        self.check_lock.acquire()
        if time() - self.last_cleanup > 60:
            highest_score = 0
            lowest_score = 99999999

            for i in range(0, len(self.websockets)):
                if self.websockets[i].score > highest_score:
                    highest_score = self.websockets[i].score
                    self.hot_id = i
                if self.websockets[i].score < lowest_score:
                    lowest_score = self.websockets[i].score
                    self.cold_id = i

            if self.ws_created < MAX_WS and self.cold_id != -1:
                self.websockets[self.cold_id].close()
                self.websockets[self.cold_id] = WebSocket(self.url, hot_handler=self.dispatcher_handler, subs=self.subs)
                gc.collect()
                self.websockets[self.cold_id].id = self.cold_id
                self.ws_created += 1

            for i in range(0, len(self.websockets)):
                self.websockets[i].score = 0
            self.last_cleanup = time()

        # warmup every CLEANUP_FREQ hours
        if time() - self.last_warmup > MAX_WARMUP_WAIT * 60 * 60:
            self.last_warmup = time()
            self.ws_created = MAX_SIMULTANEOUS_WS
        self.check_lock.release()

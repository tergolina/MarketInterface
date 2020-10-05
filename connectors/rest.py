from time import sleep, time
from datetime import datetime
from threading import Thread, Event, Lock
import requests
import json
import numpy as np
import pandas as pd
import scipy.stats as st


class Rest:
    """ Classe responsavel pelo envio de requests e gerenciamento de sessions """
    def __init__(self, url, session=False, auth=None, timeout=3):
        """ Inicializacao da classe

            Args:
                url (str): Url default que sera usada para criacao das sessions
                session (bool): Se deve selecionar a melhor session
        """
        self.timeout = timeout
        self.query_lock = Lock()
        self.url = url
        if session:
            self.session = self.create_session(3, 3)
        else:
            self.session = requests.Session()
        if (auth != None) and callable(auth):
            self.session.auth = auth()

    def create_session(self, subjects, accuracy):
        """ Cria e seleciona as melhores sessions

            Args:
                subjects (int): Numero de sessions que serao criadas para a selecao
                accuracy (int): Numero de pings que serao dados em cada session

            Returns:
                (Session): A melhor session testada
        """
        error_count = 0
        sessions = [requests.Session() for j in range(subjects)]
        diagnostic = np.zeros((len(sessions), accuracy + 1))
        for i in range(accuracy + 1):
            j = 0
            while j < len(sessions):
                try:
                    r = sessions[j].get(self.url)
                    if r.status_code == 200:
                        time_elapsed = r.elapsed.total_seconds()
                        diagnostic[j][i] = time_elapsed
                        j += 1
                        error_count = 0
                    else:
                        error_count += 1
                        if error_count > 4:
                            sessions[j] = requests.Session()
                except Exception as e:
                    error_count += 1
                    if error_count > 4:
                        sessions[j] = requests.Session()
                sleep(3)
        indexes = []
        for j in range(len(diagnostic)):
            indexes += [((st.norm.ppf(0.8) * np.std(diagnostic[j][1:])) + np.mean(diagnostic[j][1:]))*1000]
            best = list(pd.Series(indexes).sort_values().index[:1])
        return sessions[best[0]]

    def query(self, method='get', url=None, data=None, params=None, headers=None,
              auth=None, timeout=None, hooks=None, stream=None, json=None, return_elapsed=False):
        """ Envia um request

            Args:
                method (str): O tipo de request que sera enviado (get, post, put, delete...)
                url (str): A url que sera usada como base. Caso nao seja informada
                    usa-se a url informada para o construtor do objeto

            Returns:
                (json): Resposta do request
        """
        response = {}
        elapsed = None
        if url == None:
            url = self.url
        try:
            r = self.session.request(method.lower(), url, data=data, params=params,
                                     headers=headers, auth=auth, timeout=self.timeout,
                                     hooks=hooks, stream=stream, json=json)
            elapsed = r.elapsed.seconds + (r.elapsed.microseconds/1000000)
            try:
                if r.status_code == 200:
                    response = r.json()
                else:
                    response = {'error': {'code': r.status_code, 'message': r.json()}}
            except Exception as e:
                response = {'error': str(e), 'message': r}
        except Exception as e:
            print (datetime.now(), '- [ Rest ] Error requesting', method.lower())
            response = {'error': str(e), 'message': None}
        if return_elapsed:
            return {'response': response, 'elapsed': elapsed}
        else:
            return response

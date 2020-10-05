from time import sleep, time
from threading import Thread, Event, Lock


class Poll:
    """ Mantem executando em paralelo uma dada funcao em uma dada periodicidade
        ou em razao de um dado evento. """
    def __init__(self, function, args=[], frequency=None, trigger=None, notification=None, imediate=True, delay=0, n=None, wait=True):
        """ Inicializacao da classe

            Args:
                function (callable): Função a ser executada
                args (list): Parametros da funcao
                frequency (int): Frequencia em Hz em que a função deve ser executada
                trigger (Event): Quando setado, roda a função independentemente da frequencia
                notification (Event): Evento que sera setado a cada execucao
                imediate (bool): Se deve rodar imediatamente a funcao
                delay (int): Atraso em segundos antes de comecar a rodar o Poll
                n (int): Numero de Threads rodando a funcao
                wait (bool): Se espera a funcao rodar para receber um novo trigger
        """
        self.function = function
        self.args = args
        # Clocks ---------------------------------------------------------------
        self.paused = False
        self.frequency = frequency
        self.period = (1 / frequency) if (frequency != None) else None
        self.timeout = 5
        # Events ---------------------------------------------------------------
        self.trigger = trigger if isinstance(trigger, Event) else Event()
        self.notification = notification if isinstance(notification, Event) else Event()
        if imediate:
            self.trigger.set()
        # Actors ---------------------------------------------------------------
        self.wait = wait
        self.n = n if (n != None) and isinstance(n, int) else ((1 + int(frequency / 5)) if frequency != None else 1)
        self.run = [Event() for i in range(self.n)]
        self.threads = [Thread() for i in range(self.n)]
        self.th_manager = Thread(target=self.manager, args=[delay])
        self.th_manager.start()
        self.started = False
        self.th_keep_alive = Thread(target=self.keep_alive, args=[10])
        self.th_keep_alive.start()
        self.started = True

    def run_function(self, i):
        while True:
            self.run[i].wait()
            if not self.paused:
                self.run[i].clear()
                self.function(*self.args)
                self.notification.set()

    def manager(self, delay=0):
        sleep(delay)
        i = 0
        while True:
            self.trigger.wait(self.period)
            if not self.paused:
                i = (i + 1) % len(self.run)
                self.run[i].set()
                if self.wait:
                    self.notification.wait(self.timeout)
                self.trigger.clear()

    def keep_alive(self, _t):
        while True:
            if not self.paused:
                if not self.th_manager.isAlive():
                    print ('[ Poll ] MANAGER THREAD DEAD, RESTARTING.', self.function, self.args)
                    self.trigger.clear()
                    self.th_manager = Thread(target=self.manager)
                    self.th_manager.start()
                for i in range(len(self.threads)):
                    if not self.threads[i].isAlive():
                        if self.started:
                            print ('[ Poll ] RUN THREAD DEAD, RESTARTING.', self.function, self.args)
                            self.run[i].clear()
                        self.threads[i] = Thread(target=self.run_function, args=[i])
                        self.threads[i].start()
                sleep(_t)

    def pause(self):
        self.paused = True

    def play(self):
        self.paused = False

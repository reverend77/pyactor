from pyactor.engine.internal.base.actors import ReceiveTimedOut
from pyactor.engine.internal.base.messages import ExitMessage, ActorCreationMessage, ActorId, Message, ActorCreationResponse
from time import monotonic, sleep
from queue import Empty
from copy import deepcopy
from queue import Queue


class Endpoint:
    """
    Endpoint actor - allows to send messages from outside of the actor system.
    """

    def __init__(self):
        self.__id = None
        self._queue_in = Queue()
        self.__queue_out = None
        self.__pipe_semaphore = None
        self.__spawn_return_queue = Queue(maxsize=1)

    @property
    def id(self):
        return self.__id

    def terminate(self):
        self.__queue_out.put(ExitMessage())

    def spawn(self, actor_class, *args, **kwargs):
        """
        Spawns an actor and returns that actor id.
        :param actor_class:
        :param args:
        :param kwargs:
        :return:
        """
        message = ActorCreationMessage(actor_class, self.id, *args, **kwargs)
        self.__queue_out.put(message)
        actor_id = self.__spawn_return_queue.get()
        assert isinstance(actor_id, ActorId), "actor_id must be an instance of ActorId"
        return actor_id

    def receive(self, timeout=None, predicate=lambda data: True):

        def timed_out():
            if timeout is None:
                return False
            else:
                return monotonic() - start <= timeout

        leftovers = []
        start = monotonic()
        try:
            while not timed_out():
                try:
                    data = self._queue_in.get_nowait()
                    if predicate(data):
                        return data
                    else:
                        leftovers.append(data)
                except Empty:
                    self.switch()
        finally:
            for leftover in leftovers:
                self._queue_in.put(leftover)

        raise ReceiveTimedOut()

    @staticmethod
    def switch():
        sleep(0.001)

    def send_message(self, recipient, data):
        """
        Send message to another actor using its id.
        :param priority:
        :param recipient:
        :param data:
        :return:
        """
        if recipient.node_id == self.id.node_id:
            data = deepcopy(data)
        msg = Message(recipient, data)
        self.__queue_out.put(msg)

    def set_connection_properties(self, identifier, queue_out):
        self.__id = identifier
        self.__queue_out = queue_out

    def enqueue_message(self, message):
        """
        Used to put a data included in the message into the queue.
        :param message: message to be enqueued
        :return:
        """
        assert isinstance(message, Message), "Unsupported message - must be an instance of Message"
        if isinstance(message, ActorCreationResponse):
            self.__spawn_return_queue.put(message.data)
        else:
            self._queue_in.put(message.data)
from threading import Thread
from time import monotonic
from queue import Empty

from .messages import Message, PoisonPill, Broadcast


class Actor(Thread):
    """
    Basic actor class.
    """
    def __init__(self, identifier, queue_in, queue_out):
        super().__init__()
        assert isinstance(identifier, str), "identifier must be a string"
        self.id = identifier
        self.__queue_in = queue_in
        self._is_terminating = False
        self._queue_out = queue_out

    def run(self):
        """
        Override this method to implement actor behaviour
        """
        raise NotImplementedError("run method not implemented on actor {}".format(self.id))

    def enqueue_message(self, message):
        """
        Used to put a data included in the message into the queue.
        :param message: message to be enqueued
        :return:
        """
        if self._is_terminating:
            return
        assert isinstance(message, Message)
        if isinstance(message, PoisonPill):
            self.__terminate()
        elif isinstance(message, Broadcast) and message.source == self.id:
            """
            Ignore broadcast if this actor is the original source.
            """
            return
        elif self._is_data_valid(message.data):
            self.__queue_in.put(message.data)

    def _is_data_valid(self, data):
        """
        Method responsible of filtering unwanted messages.
        By default, always returns True.
        :param data: content of a message
        :return: boolean indicating whether this message should be processed
        """
        return True

    def send_message(self, recipient, data, priority=0):
        """
        Send message to another actor using its id.
        :param priority:
        :param recipient:
        :param data:
        :return:
        """
        msg = Message(recipient, data, priority=priority)
        self._queue_out.put(msg)

    def send_broadcast_message(self, data, priority=0):
        """
        Send message to every other actor in the system.
        :param priority:
        :param data:
        :return:
        """
        msg = Broadcast(data, source=self.id, priority=priority)
        self._queue_out.put(msg)

    def receive(self, timeout=None, predicate=lambda x: True):
        """
        Allows an actor to receive a message while it's running. Supports timeout and a predicate to find a message that
        is important to the actor at the given time.
        It does not override filters of is_data_valid and requeues messages that do not pass filtering by predicate arg.

        :param predicate: predicate used to filter the incoming messages (applied AFTER _is_data_valid)
        :param timeout: positive integer (seconds) or None. If timeout is exceeded, raises ReceiveTimeoutException
        :return:
        """

        def timeout_exceeded():
            if timeout is None:
                return False
            else:
                return monotonic() - start <= timeout

        non_matching = []
        start = monotonic()
        while not timeout_exceeded():
            try:
                data = self.__queue_in.get(timeout=timeout)
                if predicate(data):
                    return data
                else:
                    non_matching.append(data)
            except Empty:
                pass
            finally:
                for non_matched in non_matching:
                    self.__queue_in.put(non_matched)
        raise ReceiveTimeoutException("Matching message not found.")

    def __terminate(self):
        """
        Method to be called when an actor is terminated.
        :return:
        """
        self._is_terminating = True
        pass


class ReceiveTimeoutException(Exception):
    """
    Internal class used to represent timeout of actor.receive method.
    """
    pass

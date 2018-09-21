from threading import Thread
from time import monotonic
from queue import Empty, Queue

from pyactor.engine.messages import Message, ActorId, Broadcast, ActorCreationMessage


class Actor:
    """
    Basic actor class.
    """
    def __init__(self, identifier, queue_out, pipe_semaphore, accept_broadcasts=True):
        super().__init__()
        assert isinstance(identifier, ActorId), "identifier must be an ActorId"
        self.id = identifier
        self.__queue_in = Queue()
        self._queue_out = queue_out
        self.__accept_broadcasts = accept_broadcasts
        self._thread = None
        self.__pipe_semaphore = pipe_semaphore

    def start(self):
        self._thread = Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self):
        """
        Override this method to implement actor behaviour
        """
        raise NotImplementedError("run method not implemented on actor {}".format(self.id))

    def _enqueue_message(self, message):
        """
        Used to put a data included in the message into the queue.
        :param message: message to be enqueued
        :return:
        """
        assert isinstance(message, Message), "Unsupported message - must be an instance of Message"

        if isinstance(message, Broadcast) and (not self.__accept_broadcasts or message.source == self.id):
            """
            Ignore broadcast if this actor is the original source or if this actor does not accept broadcasts.
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

    def send_message(self, recipient, data):
        """
        Send message to another actor using its id.
        :param priority:
        :param recipient:
        :param data:
        :return:
        """
        msg = Message(recipient, data)
        self._queue_out.put(msg)

    def send_broadcast_message(self, data):
        """
        Send message to every other actor in the system.
        :param priority:
        :param data:
        :return:
        """
        msg = Broadcast(data, source=self.id)
        self._queue_out.put(msg)

    def spawn(self, actor_class, *args, **kwargs):
        """
        Spawns an actor and returns that actor id.
        :param actor_class:
        :param args:
        :param kwargs:
        :return:
        """
        with self.__pipe_semaphore:
            message = ActorCreationMessage(actor_class, *args, **kwargs)
            self._queue_out.put(message)
            receiver = message.receiver
            actor_id = receiver.recv()
            assert isinstance(actor_id, ActorId), "actor_id must be an instance of ActorId"
            receiver.close()
            return actor_id

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


class ReceiveTimeoutException(Exception):
    """
    Internal class used to represent timeout of actor.receive method.
    """
    pass

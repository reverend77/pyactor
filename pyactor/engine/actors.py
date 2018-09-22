from threading import Thread
from time import monotonic
from queue import Empty, Queue

from pyactor.engine.messages import Message, ActorId, ActorCreationMessage


class Actor:
    """
    Basic actor class.
    """
    def __init__(self, identifier, queue_out, pipe_semaphore, callback=None):
        super().__init__()
        assert isinstance(identifier, ActorId), "identifier must be an ActorId"
        self.id = identifier
        self.__queue_in = Queue()
        self._queue_out = queue_out
        self._thread = None
        self.__pipe_semaphore = pipe_semaphore
        self.__callback = callback

    def start(self):
        self._thread = Thread(target=self.__run)
        self._thread.daemon = True
        self._thread.start()

    def __run(self):
        try:
            self.run()
        finally:
            if self.__callback:
                self.__callback()

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
        self.__queue_in.put(message.data)

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

    def receive(self, block=True, timeout=None):
        return self.__queue_in.get(block=block, timeout=timeout)


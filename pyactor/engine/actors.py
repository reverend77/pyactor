from threading import Thread
from time import monotonic
from queue import Empty, Queue

from pyactor.engine.messages import Message, ActorId, ActorCreationMessage


class Actor:
    """
    Basic actor class.
    """
    def __init__(self, identifier, queue_out, pipe_semaphore):
        super().__init__()
        assert isinstance(identifier, ActorId), "identifier must be an ActorId"
        self.id = identifier
        self.__queue_in = Queue()
        self._queue_out = queue_out
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

    def receive(self):
        """
        Allows an actor to receive a message while it's running. Supports timeout and a predicate to find a message that
        is important to the actor at the given time.
        It does not override filters of is_data_valid and requeues messages that do not pass filtering by predicate arg.

        :param predicate: predicate used to filter the incoming messages (applied AFTER _is_data_valid)
        :param timeout: positive integer (seconds) or None. If timeout is exceeded, raises ReceiveTimeoutException
        :return:
        """
        return self.__queue_in.get()


from time import monotonic
from queue import Empty, Queue
import asyncio
from copy import deepcopy

from pyactor.engine.internal.base.messages import Message, ActorId, ActorCreationMessage, ActorCreationResponse


class Actor:
    """
    Basic actor class.
    """

    def __init__(self):
        self.__id = None
        self.__queue_in = Queue()
        self.__queue_out = None
        self.__pipe_semaphore = None
        self.__spawn_return_queue = Queue(maxsize=1)

    @property
    def id(self):
        return self.__id

    def set_connection_properties(self, identifier, queue_out):
        self.__id = identifier
        self.__queue_out = queue_out

    @staticmethod
    async def switch():
        await asyncio.sleep(0.01)

    def start(self, loop):
        asyncio.run_coroutine_threadsafe(self.__run(), loop)

    async def __run(self):
        await self.run()

    async def run(self):
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
        assert isinstance(message, Message), "Unsupported message - must be an instance of Message"
        if isinstance(message, ActorCreationResponse):
            self.__spawn_return_queue.put(message.data)
        else:
            self.__queue_in.put(message.data)

    async def send_message(self, recipient, data):
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
        await self.switch()

    async def spawn(self, actor_class, *args, **kwargs):
        """
        Spawns an actor and returns that actor id.
        :param actor_class:
        :param args:
        :param kwargs:
        :return:
        """
        message = ActorCreationMessage(actor_class, self.__id, *args, **kwargs)
        self.__queue_out.put(message)

        while self.__spawn_return_queue.empty():
            await self.switch()

        actor_id = self.__spawn_return_queue.get()
        assert isinstance(actor_id, ActorId), "actor_id must be an instance of ActorId"
        return actor_id

    async def receive(self, timeout=None, predicate=lambda data: True):

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
                    data = self.__queue_in.get_nowait()
                    if predicate(data):
                        return data
                    else:
                        leftovers.append(data)
                except Empty:
                    await self.switch()
        finally:
            for leftover in leftovers:
                self.__queue_in.put(leftover)

        raise ReceiveTimedOut()


class ReceiveTimedOut(Exception):
    pass


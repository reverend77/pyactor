from time import monotonic, sleep
from queue import Empty, Queue
import asyncio

from pyactor.engine.messages import Message, ActorId, ActorCreationMessage


class Actor:
    """
    Basic actor class.
    """

    def __init__(self, identifier, queue_out, pipe_semaphore, callback=None):
        super().__init__()
        assert isinstance(identifier, ActorId), "identifier must be an ActorId"
        self.id = identifier
        self._queue_in = Queue()
        self._queue_out = queue_out
        self._pipe_semaphore = pipe_semaphore
        self.__callback = callback

    async def switch(self):
        await asyncio.sleep(0.01)

    def start(self, loop):
        asyncio.run_coroutine_threadsafe(self.run(), loop)

    async def __run(self):
        try:
            await self.run()
        finally:
            if self.__callback:
                self.__callback()

    async def run(self):
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
        self._queue_in.put(message.data)

    async def send_message(self, recipient, data):
        """
        Send message to another actor using its id.
        :param priority:
        :param recipient:
        :param data:
        :return:
        """
        msg = Message(recipient, data)
        self._queue_out.put(msg)
        await self.switch()

    async def spawn(self, actor_class, *args, **kwargs):
        """
        Spawns an actor and returns that actor id.
        :param actor_class:
        :param args:
        :param kwargs:
        :return:
        """
        try:
            while not self._pipe_semaphore.acquire(False):
                await self.switch()
            message = ActorCreationMessage(actor_class, *args, **kwargs)
            self._queue_out.put(message)
            receiver = message.receiver

            while not receiver.poll():
                await self.switch()

            actor_id = receiver.recv()
            assert isinstance(actor_id, ActorId), "actor_id must be an instance of ActorId"
            receiver.close()
            return actor_id
        finally:
            self._pipe_semaphore.release()

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
                    data = self._queue_in.get_nowait()
                    if predicate(data):
                        return data
                    else:
                        leftovers.append(data)
                except Empty:
                    await self.switch()
        finally:
            for leftover in leftovers:
                self._queue_in.put(leftover)

        raise ReceiveTimedOut()


class ReceiveTimedOut(Exception):
    pass


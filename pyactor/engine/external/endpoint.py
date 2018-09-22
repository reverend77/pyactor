from pyactor.engine.actors import Actor, ReceiveTimedOut
from pyactor.engine.messages import ExitMessage, ActorCreationMessage, ActorId, Message
from time import monotonic, sleep
from queue import Empty


class Endpoint(Actor):
    """
    Endpoint actor - allows to send messages from outside of the actor system.
    """

    def __init__(self, identifier, queue_out, pipe_semaphore):
        super().__init__(identifier, queue_out, pipe_semaphore, None)

    def run(self):
        raise NotImplementedError("{} does not support run method.".format(Endpoint))

    def start(self):
        raise NotImplementedError("{} does not support start method.".format(Endpoint))

    def terminate(self):
        self._queue_out.put(ExitMessage())

    def spawn(self, actor_class, *args, **kwargs):
        """
        Spawns an actor and returns that actor id.
        :param actor_class:
        :param args:
        :param kwargs:
        :return:
        """
        try:
            self._pipe_semaphore.acquire(True)

            message = ActorCreationMessage(actor_class, *args, **kwargs)
            self._queue_out.put(message)
            receiver = message.receiver
            actor_id = receiver.recv()
            assert isinstance(actor_id, ActorId), "actor_id must be an instance of ActorId"
            receiver.close()
            return actor_id
        finally:
            self._pipe_semaphore.release()

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

    def switch(self):
        sleep(0.001)

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

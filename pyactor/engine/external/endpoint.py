from pyactor.engine.actors import Actor
from pyactor.engine.messages import ExitMessage


class Endpoint(Actor):
    """
    Endpoint actor - allows to send messages from outside of the actor system.
    """
    def __init__(self, identifier, queue_out):
        super().__init__(identifier, queue_out, accept_broadcasts=False)

    def run(self):
        raise NotImplementedError("{} does not support run method.".format(Endpoint))

    def start(self):
        raise NotImplementedError("{} does not support start method.".format(Endpoint))

    def terminate(self):
        self._queue_out.put(ExitMessage())


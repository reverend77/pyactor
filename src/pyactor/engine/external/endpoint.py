from ..actors import Actor
from ..messages import ExitMessage


class Endpoint(Actor):

    def __init__(self, identifier, queue_out):
        super().__init__(identifier, queue_out)

    def run(self):
        raise NotImplementedError("{} does not support run method.".format(Endpoint))

    def start(self):
        raise NotImplementedError("{} does not support start method.".format(Endpoint))

    def terminate(self):
        self._queue_out.put(ExitMessage())


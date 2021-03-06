from multiprocessing import Queue, Process
from os import cpu_count
from threading import Thread

from pyactor.engine.external.node import ExternalNode
from pyactor.engine.utils.node_utils import spawn_and_start_node


def _start_external_node(queue_in, other_queues_out):
    node = ExternalNode(queue_in, {id: queue for id, queue in other_queues_out.items() if id != 0})
    endpoint = node.create_endpoint()
    worker_thread = Thread(target=node.start)
    worker_thread.start()
    return endpoint


def start_system(nodes=cpu_count()):
    queues = {i: Queue() for i in range(nodes + 1)}

    for id in range(1, nodes + 1):
        proc = Process(target=spawn_and_start_node, args=(id, queues[id], queues))
        proc.start()
    return _start_external_node(queues[0], queues)


from pyactor.engine.internal.base.actors import Actor, ActorId


class TestActor(Actor):
    async def run(self):
        endpoint_pid = await self.receive(predicate=lambda d: isinstance(d, ActorId))
        await self.send_message(endpoint_pid, None)
        while True:
            child_pid = await self.spawn(self.__class__)
            await self.send_message(child_pid, endpoint_pid)


class FibonacciActor(Actor):
    def __init__(self, parent_pid, number):
        super().__init__()
        self.parent_pid = parent_pid
        self.number = number

    async def run(self):
        if self.number <= 1:
            await self.send_message(self.parent_pid, 1)
        else:
            await self.spawn(self.__class__, self.id, self.number - 1)
            await self.spawn(self.__class__, self.id, self.number - 2)
            number1 = await self.receive()
            number2 = await self.receive()
            await self.send_message(self.parent_pid, number1 + number2)


if __name__ == "__main__":
    endpoint = start_system()
    for num in range(1):
        endpoint.spawn(FibonacciActor, endpoint.id, num)
        result = endpoint.receive()
        print("{}. Fibonacci number: {}".format(num + 1, result))
    endpoint.stop_system()
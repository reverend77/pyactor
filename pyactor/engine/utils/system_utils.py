from multiprocessing import Queue, Process, Value, Lock
from os import cpu_count
from threading import Thread

from pyactor.engine.external.node import ExternalNode
from pyactor.engine.utils.node_utils import spawn_and_start_node
from pyactor.engine.utils.lock import RWLock


def _start_external_node(queue_in, other_queues_out, node_load, scheduler_lock):
    node = ExternalNode(queue_in, {id: queue for id, queue in other_queues_out.items() if id != 0}, node_load, scheduler_lock)
    endpoint = node.create_endpoint()
    Thread(target=node.start).start()
    return endpoint


def start_system(nodes=cpu_count()):
    queues = {i: Queue() for i in range(nodes + 1)}
    scheduler_lock = RWLock()

    node_load = {node_id: Value("Q", 0, lock=False) for node_id in range(1, nodes + 1)}
    for id in range(1, nodes + 1):
        proc = Process(target=spawn_and_start_node, args=(id, queues[id], queues, node_load, scheduler_lock))
        proc.start()
    return _start_external_node(queues[0], queues, node_load, scheduler_lock)


from pyactor.engine.actors import Actor, ActorId


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
    first = endpoint.spawn(TestActor)
    endpoint.send_message(first, endpoint.id)

    counter = 0
    while True:
        endpoint.receive()
        counter += 1
        print(counter)

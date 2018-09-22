from multiprocessing import Queue, Process, Semaphore
from os import cpu_count
from threading import Thread

from pyactor.engine.external.node import ExternalNode
from pyactor.engine.utils.node_utils import spawn_and_start_node


def _start_external_node(queue_in, other_queues_out, pipe_semaphore):
    node = ExternalNode(queue_in, {id: queue for id, queue in other_queues_out.items() if id != 0}, pipe_semaphore)
    endpoint = node.create_endpoint()
    Thread(target=node.start).start()
    return endpoint


def start_system(nodes=cpu_count()):
    queues = {i: Queue() for i in range(nodes + 1)}

    pipe_semaphore = Semaphore(1000)
    for id in range(1, nodes + 1):
        proc = Process(target=spawn_and_start_node, args=(id, queues[id], queues, pipe_semaphore))
        proc.start()
    return _start_external_node(queues[0], queues, pipe_semaphore)


from pyactor.engine.actors import Actor
from time import sleep

class TestActor(Actor):
    def run(self):
        endpoint_pid = self.receive()
        self.send_message(endpoint_pid, None)


if __name__ == "__main__":
    endpoint = start_system()
    pid = endpoint.spawn(TestActor)
    endpoint.send_message(pid, endpoint.id)
    endpoint.receive()
    print("Received!")
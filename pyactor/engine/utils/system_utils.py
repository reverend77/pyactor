from multiprocessing import Queue, Process
from os import cpu_count
from threading import Thread

from pyactor.engine.external.node import ExternalNode
from pyactor.engine.utils.node_utils import spawn_and_start_node


def _start_external_node(queue_in, other_queues_out):
    node = ExternalNode(queue_in, {id: queue for id, queue in other_queues_out.items() if id != 0})
    endpoint = node.create_endpoint()
    Thread(target=node.start).start()
    return endpoint


def start_system(nodes=cpu_count()):
    queues = {i: Queue() for i in range(nodes + 1)}
    for id in range(1, nodes + 1):
        proc = Process(target=spawn_and_start_node, args=(id, queues[id], queues))
        proc.start()
    return _start_external_node(queues[0], queues)


from pyactor.engine.actors import Actor
from time import sleep

class TestActor(Actor):
    def run(self):
        sleep(600)
        return

if __name__ == "__main__":
    endpoint = start_system()
    for __ in range(1000000):
        print(endpoint.spawn(TestActor))
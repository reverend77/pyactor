from multiprocessing import Queue, Process
from os import cpu_count

from ..external.node import ExternalNode
from .node_utils import spawn_and_start_node


def _start_external_node(queue_in, other_queues_out):
    node = ExternalNode(queue_in, {id: queue for id, queue in other_queues_out if id != 0})
    node.start()
    return node.create_endpoint()


def start_system(nodes=cpu_count()):
    queues = {i: Queue() for i in range(nodes + 1)}
    for id in range(1, nodes + 1):
        proc = Process(target=spawn_and_start_node, args=(id, queues[id], queues))
        proc.start()
    return _start_external_node(queues[0], queues)

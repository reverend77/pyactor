from pyactor.engine.node import Node


def spawn_and_start_node(node_id, queue_in, node_queues, pipe_semaphore, gc_interval=30):
    """
    Method that ought to be run in a separate process. This process then becomes a node.
    :param node_id:
    :param queue_in:
    :param queue_out:
    :param gc_interval:
    :return:
    """
    del node_queues[node_id]  # remove redundant reference to self
    node = Node(node_id, queue_in, node_queues, pipe_semaphore, gc_interval=gc_interval)
    node.start()

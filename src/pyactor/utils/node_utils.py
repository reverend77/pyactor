from ..engine.node import Node

NODE = None


def spawn_and_start_node(node_id, queue_in, queue_out, gc_interval=30):
    """
    Method that ought to be run in a separate process. This process then becomes a node.
    :param node_id:
    :param queue_in:
    :param queue_out:
    :param gc_interval:
    :return:
    """
    global NODE
    if NODE is None:
        NODE = Node(node_id, queue_in, queue_out, gc_interval=gc_interval)
        NODE.start()

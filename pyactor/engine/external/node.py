from pyactor.engine.node import Node
from pyactor.engine.messages import Message, ExitMessage, ActorCreationMessage
from pyactor.engine.external.endpoint import Endpoint
import weakref
from queue import Empty


class ExternalNode(Node):
    def __init__(self, queue_in, other_nodes, node_load, pipe_semaphore):
        super().__init__(0, queue_in, other_nodes, node_load, pipe_semaphore)

    def _handle_internal_message(self):
        """
        Handle a message that came from local actor.
        :return:
        """
        try:
            msg = self._internal_queue_in.get(block=False)
            if isinstance(msg, ExitMessage):
                for queue in self._other_nodes.values():
                    queue.put(msg)
                self.terminate()

            assert isinstance(msg, Message), "Message must be an instance of Message class"
        except Empty:
            return False

        if isinstance(msg, ActorCreationMessage):
            """
            An actor will be spawned, but not on external node.
            """
            self._enqueue_actor_spawn_message(msg)

        elif msg.recipient.node_id == self._id:
            self._send_message_to_local_recipient(msg)

        else:
            self._send_message_to_remote_recipient(msg)
        return True

    def create_endpoint(self):
        # TODO endpoint cleanup
        actor_id = self._next_actor_id()
        endpoint = Endpoint(self._node_load)
        endpoint.set_connection_properties(actor_id, self._internal_queue_in, self._pipe_semaphore)
        with self._lock:
            self._actors[actor_id] = weakref.ref(endpoint)
        return endpoint

    def _get_actor_by_id(self, id):
        ref = super()._get_actor_by_id(id)
        if ref:
            return ref()
        else:
            return None

from ..node import Node
from ..messages import Message, ExitMessage, ActorCreationMessage, Broadcast, ActorId
from .endpoint import Endpoint

from queue import Empty
from random import choice

EXTERNAL_NODE_ID = 0


class ExternalNode(Node):
    def __init__(self, queue_in, other_nodes):
        super().__init__(EXTERNAL_NODE_ID, queue_in, other_nodes)

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
            return

        if isinstance(msg, ActorCreationMessage):
            """
            An actor will be spawned, but it has yet to be determined where to spawn it.
            """
            self._enqueue_actor_spawn_message(msg)

        elif isinstance(msg, Broadcast):
            self.__send_message_to_remote_recipient(msg)
            self.__broadcast_message_locally(msg)

        elif msg.recipient.node_id == self._id:
            self.__send_message_to_local_recipient(msg)

        else:
            self.__send_message_to_remote_recipient(msg)

    def _enqueue_actor_spawn_message(self, msg):
        chosen_queue = choice(self._other_nodes.values())
        chosen_queue.put(msg)

    def create_endpoint(self):
        actor_id = self._next_actor_id()
        return Endpoint(actor_id, self._internal_queue_in)

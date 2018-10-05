from pyactor.engine.internal.base.messages import Message, ExitMessage, ActorCreationMessage
from pyactor.engine.external.endpoint import Endpoint
from queue import Empty, Queue
from threading import RLock, Thread
from time import sleep, monotonic
from itertools import cycle

from pyactor.engine.internal.base.messages import ActorId


class ExternalNode:
    def __init__(self, queue_in, other_nodes):
        super().__init__()
        self._id = 0
        self._external_queue_in = queue_in
        self._other_nodes = other_nodes
        self._internal_queue_in = Queue()

        self._actors = {}
        self._lock = RLock()
        self._alive = True

        actor_spawning_queues = {id:queue for id, queue in other_nodes.items() if id != 0} # 0 is id of external node
        if self._id != 0:
            actor_spawning_queues[self._id] = queue_in
        self._actor_spawning_queues = actor_spawning_queues
        node_ids = list(actor_spawning_queues.keys())
        self._spawning_schedule = cycle(node_ids[self._id:] + node_ids[:self._id])
        self._actor_spawning_queues = actor_spawning_queues
        self.__worker = Thread(target=self.__start)

    def start(self):
        self.__worker.start()

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

    @staticmethod
    def terminate():
        """
        Exits the system - actors are daemons, so exit is almost immediate.
        Important notice: exit it not grateful by default.
        :return:
        """
        exit(0)

    def enqueue_message(self, message):
        self._external_queue_in.put(message)

    def __start(self):
        while True:
            internal_message_received = self._handle_internal_message()
            external_message_received = False
            for __ in self._other_nodes:
                external_message_received = self._handle_external_message()
                if not external_message_received:
                    break
            if not (internal_message_received or external_message_received):
                sleep(0.2)

    def _enqueue_actor_spawn_message(self, msg):
        """
        Select a random node and orders it to spawn an actor inside.
        :param msg:
        :return:
        """
        chosen_node_id = next(self._spawning_schedule)

        chosen_queue = self._actor_spawning_queues[chosen_node_id]
        chosen_queue.put(msg)

    def _handle_external_message(self):
        """
        Handle a message that came from external node.
        :return:
        """
        try:
            msg = self._external_queue_in.get(block=False)
            if isinstance(msg, ExitMessage):
                self.terminate()

            assert isinstance(msg, Message), "Message must be an instance of Message class"
        except Empty:
            return False

        self._send_message_to_local_recipient(msg)
        return True

    def _send_message_to_local_recipient(self, msg):
        """
        Sends a message to an actor belonging to current node.
        :param msg:
        :return:
        """
        with self._lock:
            ref = self._get_actor_by_id(msg.recipient)
            if ref:  # is there such an actor?
                ref.enqueue_message(msg)

    def _send_message_to_remote_recipient(self, msg):
        """
        Sends a message to an actor that does not belong to current node.
        :param msg:
        :return:
        """
        queue = self._other_nodes.get(msg.recipient.node_id, None)
        if queue is not None:
            queue.put(msg)

    def _next_actor_id(self):
        internal_id = int(monotonic() * 1e9)
        actor_id = ActorId(self._id, internal_id)
        with self._lock:
            while actor_id in self._actors:
                actor_id.actor_id -= 1
        return actor_id

    def _get_actor_by_id(self, id):
        with self._lock:
            return self._actors.get(id, None)

    def create_endpoint(self):
        # TODO endpoint cleanup
        actor_id = self._next_actor_id()
        endpoint = Endpoint()
        endpoint.set_connection_properties(actor_id, self._internal_queue_in)
        with self._lock:
            self._actors[actor_id] = endpoint
        return endpoint

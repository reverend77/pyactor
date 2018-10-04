from queue import Queue
from queue import Empty
from threading import RLock, Thread
from time import sleep, monotonic
from sys import exit
import asyncio

from pyactor.engine.messages import Message, ActorCreationMessage, ActorId, ExitMessage, ActorCreationResponse


class Node:
    def __init__(self, node_id, queue_in, other_nodes, node_load, scheduler_lock):
        super().__init__()
        self._id = node_id
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
        self._node_load = node_load

        self._event_loop_thread = Thread(target=self.__event_loop_method)
        self._event_loop_thread.daemon = True
        self._scheduler_lock = scheduler_lock

        self._event_loop = None

    def __event_loop_method(self):
        loop = asyncio.new_event_loop()
        self._event_loop = loop
        asyncio.set_event_loop(loop)
        loop.run_forever()

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

    def start(self):
        self._event_loop_thread.start()
        self.__start()

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

    def _handle_internal_message(self):
        """
        Handle a message that came from local actor.
        :return:
        """
        try:
            msg = self._internal_queue_in.get(block=False)

            assert isinstance(msg, Message), "Message must be an instance of Message class"
        except Empty:
            return False

        if isinstance(msg, ActorCreationMessage):
            """
            An actor will eventually be spawned, but it has yet to be determined where to spawn it.
            """
            self._enqueue_actor_spawn_message(msg)

        elif msg.recipient.node_id == self._id:
            self._send_message_to_local_recipient(msg)

        else:
            self._send_message_to_remote_recipient(msg)
        return True

    def _enqueue_actor_spawn_message(self, msg):
        """
        Select a random node and orders it to spawn an actor inside.
        :param msg:
        :return:
        """
        with self._scheduler_lock:
            chosen_node_id = min(self._node_load.keys(), key=lambda k: self._node_load[k].value)
            self._node_load[chosen_node_id].value += 1
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

        if isinstance(msg, ActorCreationMessage):
            """
            Actor creation messages can only be handled if coming from external queue.
            """
            self.__spawn_actor(msg)
        else:
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

    def __spawn_actor(self, msg):
        cls = msg.actor_class
        args = msg.args
        kwargs = msg.kwargs
        with self._lock:
            actor_id = self._next_actor_id()

            def remove_actor_ref():
                with self._scheduler_lock:
                    self._node_load[self._id].value -= 1
                with self._lock:
                    del self._actors[actor.id]

            actor = cls(*args, *kwargs)
            actor.set_connection_properties(actor_id, self._internal_queue_in, callback=remove_actor_ref)
            self._actors[actor.id] = actor

            while self._event_loop is None:
                sleep(0.001)
            actor.start(self._event_loop)

        self._internal_queue_in.put(ActorCreationResponse(msg.source, actor_id))

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




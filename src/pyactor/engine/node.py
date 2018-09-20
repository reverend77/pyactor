from multiprocessing import Queue
from queue import Empty
from threading import Thread, RLock
from time import sleep, monotonic_ns
import weakref
from sys import exit
from random import choice

from .messages import Message, Broadcast, ActorCreationMessage, ActorId, ExitMessage


# TODO make nodes communicate without a central process
class Node(Thread):
    def __init__(self, node_id, queue_in, other_nodes, gc_interval=30):
        super().__init__()
        assert node_id > 0, "node_id must be a positive integer"
        self._id = node_id
        self._external_queue_in = queue_in
        self._other_nodes = other_nodes
        self._gc_interval = gc_interval
        self._internal_queue_in = Queue()

        self._actors = {}  # actor_id -> weak refs to actors
        self._lock = RLock()
        self._alive = True

        all_node_queues = [queue for queue in other_nodes.values()]
        all_node_queues.append(queue_in)
        self._all_node_queues = all_node_queues

    def run(self):
        """
        Garbage collector thread - removes references to terminated actors.
        :return:
        """
        while self._alive:
            sleep(self._gc_interval)
            self.__gc()

    def __gc(self):
        """
        Garbage collector.
        :return:
        """
        with self._lock:
            for actor_id in (actor for actor, ref in self._actors if ref() is None):
                del self._actors[actor_id]

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
        super().start()
        while True:
            self._handle_internal_message()
            for __ in self._other_nodes:
                self._handle_external_message()

    def _handle_internal_message(self):
        """
        Handle a message that came from local actor.
        :return:
        """
        try:
            msg = self._internal_queue_in.get(block=False)

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
        """
        Select a random node and orders it to spawn an actor inside.
        :param msg:
        :return:
        """
        chosen_queue = choice(self._all_node_queues)
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
            return

        if isinstance(msg, ActorCreationMessage):
            """
            Actor creation messages can only be handled if coming from external queue.
            """
            self.__spawn_actor(msg)

        elif isinstance(msg, Broadcast):
            self.__broadcast_message_locally(msg)

        else:
            self.__send_message_to_local_recipient(msg)

    def __send_message_to_local_recipient(self, msg):
        """
        Sends a message to an actor belonging to current node.
        :param msg:
        :return:
        """
        with self._lock:
            ref = self._actors.get(msg.recipient, None)
            if ref:  # is there such an actor?
                ref = ref()
                if ref:  # is actor thread still active?
                    ref._enqueue_message(msg)
                else:
                    del self._actors[msg.recipient]

    def __send_message_to_remote_recipient(self, msg):
        """
        Sends a message to an actor that does not belong to current node.
        :param msg:
        :return:
        """
        if isinstance(msg, Broadcast):
            for queue in self._other_nodes.values():
                queue.put(msg)
        else:
            queue = self._other_nodes.get(msg.recipient.node_id, None)
            if queue is not None:
                queue.put(msg)

    def __spawn_actor(self, msg):
        cls = msg.actor_class
        args = msg.args
        kwargs = msg.kwargs
        with self._lock:
            actor = cls(self._next_actor_id(), self._internal_queue_in, *args, *kwargs)
            self._actors[actor.id] = weakref.ref(actor)
        actor.start()

        sender = msg.sender
        sender.send(actor.id) # return actor id to the caller
        sender.close()

    def _next_actor_id(self):
        internal_id = monotonic_ns()
        actor_id = ActorId(self._id, internal_id)
        with self._lock:
            while actor_id in self._actors:
                actor_id.actor_id -= 1
        return actor_id

    def __broadcast_message_locally(self, msg):
        """
        Used to broadcast a Broadcast message to all actors belonging to current process.
        :param msg:
        :return:
        """
        with self._lock:
            for actor_id, ref in self._actors:
                ref = ref()
                if ref:
                    ref._enqueue_message(msg)
                else:
                    del self._actors[actor_id]





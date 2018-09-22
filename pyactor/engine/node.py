from queue import Queue
from queue import Empty
from threading import Thread, RLock
from time import sleep, monotonic
import weakref
from sys import exit
from random import choice

from pyactor.engine.messages import Message, ActorCreationMessage, ActorId, ExitMessage


class Node:
    def __init__(self, node_id, queue_in, other_nodes, pipe_semaphore, gc_interval=30):
        super().__init__()
        self._id = node_id
        self._external_queue_in = queue_in
        self._other_nodes = other_nodes
        self._gc_interval = gc_interval
        self._internal_queue_in = Queue()

        self._actors = {}  # actor_id -> weak refs to actors
        self._lock = RLock()
        self._alive = True
        self._pipe_semaphore = pipe_semaphore

        actor_spawning_queues = [queue for id, queue in other_nodes.items() if id != 0] # 0 is id of external node
        if self._id != 0:
            actor_spawning_queues.append(queue_in)
        self._actor_spawning_queues = actor_spawning_queues

        self._thread = Thread(target=self.run)
        self._thread.daemon = True

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
            for actor_id in [actor for actor, ref in self._actors.items() if ref() is None]:
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
        self._thread.start()
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
        chosen_queue = choice(self._actor_spawning_queues)
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
            ref = self._actors.get(msg.recipient, None)
            if ref:  # is there such an actor?
                ref = ref()
                if ref:  # is actor thread still active?
                    ref._enqueue_message(msg)
                else:
                    del self._actors[msg.recipient]

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
            actor = cls(self._next_actor_id(), self._internal_queue_in, self._pipe_semaphore, *args, *kwargs)
            self._actors[actor.id] = weakref.ref(actor)
        actor.start()

        sender = msg.sender
        sender.send(actor.id) # return actor id to the caller
        sender.close()

    def _next_actor_id(self):
        internal_id = int(monotonic() * 1e9)
        actor_id = ActorId(self._id, internal_id)
        with self._lock:
            while actor_id in self._actors:
                actor_id.actor_id -= 1
        return actor_id





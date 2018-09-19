from multiprocessing import Queue
from queue import Empty
from threading import Thread, RLock
from time import sleep
import weakref

from .messages import PoisonPill, Broadcast, ActorCreationMessage


# TODO make nodes communicate without a central process
class Node(Thread):
    def __init__(self, node_id, queue_in, queue_out, gc_interval=30):
        super().__init__()
        self._id = node_id
        self._external_queue_in = queue_in
        self._queue_out = queue_out
        self._gc_interval = gc_interval
        self._internal_queue_in = Queue()

        self._actors = {}  # actor_id -> weak refs to actors
        self._lock = RLock()
        self._alive = True

    def run(self):
        while self._alive:
            sleep(self._gc_interval)
            self.gc()

    def gc(self):
        with self._lock:
            for actor_id in (actor for actor, ref in self._actors if ref() is None):
                del self._actors[actor_id]

    def terminate(self):
        self._alive = False
        poisonpill = PoisonPill()
        for ref in (actor for actor, ref in self._actors if ref() is not None):
            ref.enqueue_message(poisonpill)
        self.join()
        self.gc()

    def enqueue_message(self, message):
        if self._alive:
            self._external_queue_in.put(message)

    def start(self):
        super().start()
        while self._alive:
            self.__handle_internal_message()
            self.__handle_external_message()

    def __handle_internal_message(self):
        """
        Handle a message that came from local actor.
        :return:
        """
        try:
            msg = self._internal_queue_in.get(block=False)
        except Empty:
            return
        if isinstance(msg, ActorCreationMessage):
            self._queue_out.put(msg)
        elif isinstance(msg, Broadcast):
            self._queue_out.put(msg)
            self.__broadcast_message(msg)
        elif msg.recipient in self._actors:
            self.__send_message_to_local_recipient(msg)
        else:
            self._queue_out.put(msg)

    def __handle_external_message(self):
        try:
            msg = self._external_queue_in.get(block=False)
        except Empty:
            return
        if isinstance(msg, ActorCreationMessage):
            """
            Actor creation messages can only be handled if coming from external source.
            """
            self.__spawn_actor(msg)
        elif isinstance(msg, Broadcast):
            self.__broadcast_message(msg)
        else:
            self.__send_message_to_local_recipient(msg)

    def __send_message_to_local_recipient(self, msg):
        with self._lock:
            ref = self._actors.get(msg.recipient, None)
            if ref:  # is there such an actor?
                ref = ref()
                if ref:  # is actor thread still active?
                    ref.enqueue_message(msg)
                else:
                    del self._actors[msg.recipient]

    def __spawn_actor(self, msg):
        cls = msg.actor_class
        args = msg.args
        kwargs = msg.kwargs
        with self._lock:
            actor = cls(self.__next_actor_id(), *args, *kwargs)
            self._actors[actor.id] = weakref.ref(actor)
        actor.start()

    def __next_actor_id(self):
        raise NotImplementedError

    def __broadcast_message(self, msg):
        """
        Used to broadcast a Broadcast message to all actors belonging to current process.
        :param msg:
        :return:
        """
        with self._lock:
            for actor_id, ref in self._actors:
                ref = ref()
                if ref:
                    ref.enqueue_message(msg)
                else:
                    del self._actors[actor_id]





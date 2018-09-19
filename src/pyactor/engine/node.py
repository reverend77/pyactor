from multiprocessing import Queue
from threading import Thread
import weakref


class Node(Thread):
    def __init__(self, queue_in, queue_out, gc_interval=30):
        super().__init__()
        self._queue_in = queue_in
        self._queue_out = queue_out

        self._actors = {}  # actor_id -> weak refs to actors

    def run(self):
        for actor_id in (actor for actor, ref in self._actors if ref() is None):
            del self._actors[actor_id]

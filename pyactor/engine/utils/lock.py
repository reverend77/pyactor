from multiprocessing import Lock, Value


class RWLock:
    def __init__(self):
        self.r = Lock()
        self.b = Value("Q", 0, lock=False)
        self.g = Lock()

    def lock_read(self):
        self.r.acquire()
        self.b.value += 1
        if self.b.value == 1:
            self.g.acquire()
        self.r.release()

    def unlock_read(self):
        self.r.acquire()
        self.b.value -= 1
        if self.b.value == 0:
            self.g.release()
        self.r.release()

    def lock_write(self):
        self.g.acquire()

    def unlock_write(self):
        self.g.release()

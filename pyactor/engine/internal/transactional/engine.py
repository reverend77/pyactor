from multiprocessing import Lock
from asyncio import sleep


class TransactionEngine:
    def __init__(self, lock: Lock):
        self.lock = lock
        self.transaction_owner = None

    async def begin_transaction(self, actor_id):
        try:
            while self.transaction_owner is not None or not self.lock.acquire(blocking=False):
                await sleep(0.001)

            self.transaction_owner = actor_id
        finally:
            self.lock.release()

    async def end_transaction(self, actor_id):
        if actor_id == self.transaction_owner:
            self.transaction_owner = None





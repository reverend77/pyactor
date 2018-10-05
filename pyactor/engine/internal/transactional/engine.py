from multiprocessing import Lock
from asyncio import sleep
from contextlib import AbstractAsyncContextManager


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

    def create_manager(self, actor_id):
        return TransactionManager(actor_id, self)


class TransactionManager(AbstractAsyncContextManager):
    def __init__(self, actor_id, engine):
        self.owner = actor_id
        self.engine = engine

    async def __aenter__(self):
        await self.engine.begin_transaction(self.owner)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.engine.end_transaction(self.owner)

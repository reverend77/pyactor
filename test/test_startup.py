from pyactor.engine.internal.base.actors import Actor
from pyactor.engine.utils.system_utils import start_system


class PingActor(Actor):

    async def run(self):
        pid = await self.receive()
        await self.send_message(pid, self.id)


def test_start():
    endpoint = start_system(1)
    assert endpoint is not None

    pinged = endpoint.spawn(PingActor)
    endpoint.send_message(pinged, endpoint.id)
    pinged_id = endpoint.receive()
    assert pinged_id == pinged
    endpoint.stop_system()

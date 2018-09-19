class Message:
    """
    Basic message class used internally by the actor system.
    """

    def __init__(self, recipient, data, priority=0):
        """
        :param recipient: id of the recipient
        :param data: content of message
        :param priority: positive integer, lower value means higher priority
        """
        assert isinstance(priority, int) and priority >= 0, "priority must be a positive integer"
        assert isinstance(recipient, str), "recipient must be a string identifier"
        self.data = data
        self.recipient = recipient
        self._priority = priority

    def __repr__(self):
        return "Message({},{},{})".format(self.recipient,self.data, self._priority)

    def __lt__(self, other):
        if isinstance(other, Message):
            return self._priority < other._priority
        raise NotImplementedError


class Broadcast(Message):
    """
    Utility class used to send a message to every actor in the system at the same time.
    """
    def __init__(self, data, priority=0):
        super().__init__(None, data, priority=priority)


class PoisonPill(Message):
    """
    Basic message that can be used to stop an actor.
    """
    def __init__(self, priority=0):
        super().__init__(None, None, priority=priority)


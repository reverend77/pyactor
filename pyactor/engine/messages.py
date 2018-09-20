from multiprocessing import Pipe


class ActorId:
    """
    Class used to identify an actor in the system.
    """
    def __init__(self, node_id, actor_id):
        assert isinstance(node_id, int), "Node id must be an integer"
        assert isinstance(actor_id, int), "Actor id must be an integer"

        self.node_id = node_id
        self.actor_id = actor_id

    def __eq__(self, other):
        if isinstance(other, ActorId):
            return other.node_id == self.node_id and self.actor_id == other.actor_id
        return False

    def __hash__(self):
        return hash((self.node_id, self.actor_id))


class Message:
    """
    Basic message class used internally by the actor system.
    """

    def __init__(self, recipient, data):
        """
        :param recipient: id of the recipient
        :param data: content of message
        """
        assert isinstance(recipient, ActorId) or recipient is None, "recipient must be a string identifier"
        self.data = data
        self.recipient = recipient

    def __repr__(self):
        return "Message({},{})".format(self.recipient, self.data)


class ExitMessage:
    pass


class Broadcast(Message):
    """
    Utility class used to send a message to every actor in the system at the same time.
    """
    def __init__(self, data, source):
        super().__init__(None, data)
        self.source = source


class ActorCreationMessage(Message):
    """
    Internally used message - ought to create an actor
    """
    def __init__(self, actor_class, *args, **kwargs):
        super().__init__(None, None)
        self.actor_class = actor_class
        self.args = args
        self.kwargs = kwargs
        self.receiver, self.sender = Pipe(False)


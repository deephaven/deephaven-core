import abc
from abc import ABC
import dataclasses
from pydeephaven.proto import ticket_pb2


@dataclasses.dataclass
class ServerObject:
    """
    Implementations of this type are client-side representations of objects that actually exist on the Deephaven server.
    Presently used to enable the client API users to send and receive references to server-side plugins.

    Marked as "experimental" for now, as we work to improve the client plugin experience.
    """
    type_: str
    ticket: bytes
    def typed_ticket(self) -> ticket_pb2.TypedTicket:
        return ticket_pb2.TypedTicket(type=self.type_, ticket=self.ticket)
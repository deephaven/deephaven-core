#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import dataclasses
from pydeephaven.proto import ticket_pb2
from typing import Optional


"""
A simple supertype to use when defining objects that exist on the server which can be referenced by the client.
"""


@dataclasses.dataclass
class ServerObject:
    """
    Implementations of this type are client-side representations of objects that actually exist on the Deephaven server.
    Presently used to enable the client API users to send and receive references to server-side plugins.

    Marked as "experimental" for now, as we work to improve the client plugin experience.
    """

    type_: Optional[str]
    """The type of the object. May be None, indicating that the instance cannot be connected to or otherwise directly
    used from the client."""

    ticket: ticket_pb2.Ticket
    """The ticket that points to the object on the server."""

    def typed_ticket(self) -> ticket_pb2.TypedTicket:
        """
        Returns a typed ticket, suitable for use in communicating with an ObjectType plugin on the server.
        """
        return ticket_pb2.TypedTicket(type=self.type_, ticket=self.ticket)

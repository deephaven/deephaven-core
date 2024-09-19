#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import Optional

from pydeephaven.proto import ticket_pb2
from pydeephaven.ticket import Ticket, _ticket_from_proto


class ServerObject:
    """ A ServerObject is a typed ticket that represents objects existing on the server that can be referenced by the
    client. It is presently used to enable client API users to send and receive references to server-side plugins.
    """
    type: Optional[str]
    """The type of the object. May be None, indicating that the instance cannot be connected to or otherwise directly
    used from the client."""

    ticket: Ticket
    """The ticket that points to the object on the server."""

    def __init__(self, type: Optional[str], ticket: Ticket):
        self.type = type
        self.ticket = ticket

    @property
    def pb_ticket(self) -> ticket_pb2.Ticket:
        """Returns the ticket as a gRPC protobuf ticket object."""
        return self.ticket.pb_ticket

    @property
    def pb_typed_ticket(self) -> ticket_pb2.TypedTicket:
        """Returns a protobuf typed ticket, suitable for use in communicating with an ObjectType plugin on the server.
        """
        return ticket_pb2.TypedTicket(type=self.type, ticket=self.pb_ticket)


def _server_object_from_proto(typed_ticket: ticket_pb2.TypedTicket) -> ServerObject:
    """ Creates a ServerObject from a gRPC protobuf typed ticket object.
    """
    return ServerObject(type=typed_ticket.type, ticket=_ticket_from_proto(typed_ticket.ticket))

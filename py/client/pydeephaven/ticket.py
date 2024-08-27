#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional
from uuid import uuid4

from pydeephaven.dherror import DHError
from pydeephaven.proto import ticket_pb2


class Ticket(ABC):
    """A Ticket object references an object on the server. """
    _ticket_bytes: bytes

    @abstractmethod
    def __init__(self, ticket_bytes: bytes):
        self._ticket_bytes = ticket_bytes

    @property
    def bytes(self) -> bytes:
        """ Returns the ticket as raw bytes. """
        return self._ticket_bytes

    @property
    def pb_ticket(self) -> ticket_pb2.Ticket:
        """ Returns the ticket as a gRPC protobuf ticket object. """
        return ticket_pb2.Ticket(ticket=self._ticket_bytes)


class SharedTicket(Ticket):
    """ A SharedTicket object represents a ticket that can be shared with other sessions. """

    def __init__(self, ticket_bytes: bytes):
        """Initializes a SharedTicket object

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('SharedTicket: ticket_bytes is None')
        elif not ticket_bytes.startswith(b'h'):
            raise DHError(f'SharedTicket: ticket {ticket_bytes} is not a shared ticket')
        super().__init__(ticket_bytes)

    @classmethod
    def random_ticket(cls) -> SharedTicket:
        """Generates a random shared ticket. To minimize the probability of collision, the ticket is made from a
        generated UUID.

        Returns:
            a SharedTicket object
        """
        bytes_ = uuid4().int.to_bytes(16, byteorder='little', signed=False)
        return cls(ticket_bytes=b'h' + bytes_)


class ExportTicket(Ticket):
    """An ExportTicket is a ticket that references an object exported from the server such as a table or a plugin widget.
    An exported server object will remain available on the server until the ticket is released or the session is
    closed. Many types of server objects are exportable and can be fetched to the client as an export ticket or as an
    instance of wrapper class (such as :class:`table.Table`, :class:`plugin_client.PluginClient`，etc. that wraps the
    export ticket. An export ticket can be published to a :class:`.SharedTicket` so that the exported server object can
    be shared with other sessions.

    Note: Users should not create ExportTicket objects directly. They are managed by the Session object and are
    automatically created when exporting objects from the server via. :meth:`.Session.open_table`, :meth:`.Session.fetch`,
    and any operations that returns a Table object.
    """

    def __init__(self, ticket_bytes: bytes):
        """Initializes an ExportTicket object

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('ExportTicket: ticket is None')
        elif not ticket_bytes.startswith(b'e'):
            raise DHError(f'ExportTicket: ticket {ticket_bytes} is not an export ticket')

        super().__init__(ticket_bytes)


class ScopeTicket(Ticket):
    """A ScopeTicket is a ticket that references a scope variable on the server. Scope variables are ones in the global
    scope of the server and are accessible to all sessions. Scope variables can be fetched to the client as an export
    ticket or a Deephaven :class:`table.Table` that wraps the export ticket. """

    def __init__(self, ticket_bytes: bytes):
        """Initializes a ScopeTicket object

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('ScopeTicket: ticket is None')
        elif not ticket_bytes.startswith(b's/'):
            raise DHError(f'ScopeTicket: ticket {ticket_bytes} is not a scope ticket')

        super().__init__(ticket_bytes)

    @classmethod
    def scope_ticket(cls, name: str) -> ScopeTicket:
        """Creates a scope ticket that references a scope variable by name.

        Args:
            name (str): the name of the scope variable

        Returns:
            a ScopeTicket object
        """
        return cls(ticket_bytes=f's/{name}'.encode(encoding='ascii'))


def _ticket_from_proto(ticket: ticket_pb2.Ticket) -> Ticket:
    """Creates a Ticket object from a gRPC protobuf ticket object.

    Args:
        ticket (ticket_pb2.Ticket): the gRPC protobuf ticket object

    Returns:
        a Ticket object

    Raises:
        DHError: if the ticket type is unknown
    """
    ticket_bytes = ticket.ticket
    if ticket_bytes.startswith(b'h'):
        return SharedTicket(ticket_bytes)
    elif ticket_bytes.startswith(b'e'):
        return ExportTicket(ticket_bytes)
    elif ticket_bytes.startswith(b's/'):
        return ScopeTicket(ticket_bytes)
    else:
        raise DHError(f'Unknown ticket type: {ticket_bytes}')


class ServerObject(Ticket):
    """ A ServerObject is a typed ticket tha represents objects that exist on the server which can be referenced by the
    client. It is presently used to enable the client API users to send and receive references to server-side plugins.
    """

    type: Optional[str]
    """The type of the object. May be None, indicating that the instance cannot be connected to or otherwise directly
    used from the client."""

    def __init__(self, type: Optional[str], ticket: Ticket):
        super().__init__(ticket.bytes)
        self.type = type

    @property
    def pb_typed_ticket(self) -> ticket_pb2.TypedTicket:
        """Returns a protobuf typed ticket, suitable for use in communicating with an ObjectType plugin on the server.
        """
        return ticket_pb2.TypedTicket(type=self.type, ticket=self.pb_ticket)


def _server_object_from_proto(typed_ticket: ticket_pb2.TypedTicket) -> ServerObject:
    """ Creates a ServerObject from a gRPC protobuf typed ticket object. """
    return ServerObject(type=typed_ticket.type, ticket=_ticket_from_proto(typed_ticket.ticket))

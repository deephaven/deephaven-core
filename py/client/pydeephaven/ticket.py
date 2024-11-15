#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional
from uuid import uuid4

from pydeephaven.dherror import DHError
from deephaven_core.proto import ticket_pb2


class Ticket(ABC):
    """A Ticket references an object on the server. """
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
        """ Returns the ticket as a gRPC protobuf ticket. """
        return ticket_pb2.Ticket(ticket=self._ticket_bytes)


class SharedTicket(Ticket):
    """ A SharedTicket is a ticket that can be shared with other sessions. """

    def __init__(self, ticket_bytes: bytes):
        """Initializes a SharedTicket.

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
            a SharedTicket
        """
        bytes_ = uuid4().int.to_bytes(16, byteorder='little', signed=False)
        return cls(ticket_bytes=b'h' + bytes_)


class ExportTicket(Ticket):
    """An ExportTicket is a ticket that references an object exported from the server such as a table or a plugin widget.
    An exported server object will remain available on the server until the ticket is released or the session is
    closed. Many types of server objects are exportable and can be fetched to the client as an export ticket or as an
    instance of wrapper class (such as :class:`~.table.Table`, :class:`~.plugin_client.PluginClient`, etc.) that wraps the
    export ticket. An export ticket can be published to a :class:`.SharedTicket` so that the exported server object can
    be shared with other sessions.

    Note: Users should not create ExportTickets directly. They are managed by the Session and are automatically created
    when exporting objects from the server via. :meth:`.Session.open_table`, :meth:`.Session.fetch`, and any operations
    that return a Table.
    """

    def __init__(self, ticket_bytes: bytes):
        """Initializes an ExportTicket.

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('ExportTicket: ticket is None')
        elif not ticket_bytes.startswith(b'e'):
            raise DHError(f'ExportTicket: ticket {ticket_bytes} is not an export ticket')

        super().__init__(ticket_bytes)

    @classmethod
    def export_ticket(cls, ticket_no: int) -> ExportTicket:
        """Creates an export ticket from a ticket number.

        Args:
            ticket_no (int): the export ticket number

        Returns:
            an ExportTicket
        """
        ticket_bytes = ticket_no.to_bytes(4, byteorder='little', signed=True)
        return cls(b'e' + ticket_bytes)


class ScopeTicket(Ticket):
    """A ScopeTicket is a ticket that references a scope variable on the server. Scope variables are variables in the global
    scope of the server and are accessible to all sessions. Scope variables can be fetched to the client as an export
    ticket or a Deephaven :class:`~.table.Table` that wraps the export ticket. """

    def __init__(self, ticket_bytes: bytes):
        """Initializes a ScopeTicket.

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
            a ScopeTicket
        """
        if not name:
            raise DHError('scope_ticket: name must be a non-empty string')

        return cls(ticket_bytes=f's/{name}'.encode(encoding='ascii'))


class ApplicationTicket(Ticket):
    """An ApplicationTicket is a ticket that references a field of an application on the server. Please refer to the
    documentation on 'Application Mode' for detailed information on applications and their use cases."""

    def __init__(self, ticket_bytes: bytes):
        """Initializes an ApplicationTicket.

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('ApplicationTicket: ticket is None')
        elif not ticket_bytes.startswith(b'a/'):
            raise DHError(f'ApplicationTicket: ticket {ticket_bytes} is not an application ticket')
        elif len(ticket_bytes.split(b'/')) != 4:
            raise DHError(f'ApplicationTicket: ticket {ticket_bytes} is not in the correct format')

        self.app_id = ticket_bytes.split(b'/')[1].decode(encoding='ascii')
        self.field = ticket_bytes.split(b'/')[3].decode(encoding='ascii')

        super().__init__(ticket_bytes)

    @classmethod
    def app_ticket(cls, app_id: str, field: str) -> ApplicationTicket:
        """Creates an application ticket that references a field of an application.

        Args:
            app_id (str): the application id
            field (str): the name of the application field

        Returns:
            an ApplicationTicket
        """
        if not app_id:
            raise DHError('app_ticket: app_id must be a non-empty string')
        if not field:
            raise DHError('app_ticket: field must be a non-empty string')

        return cls(ticket_bytes=f'a/{app_id}/f/{field}'.encode(encoding='ascii'))


def _ticket_from_proto(ticket: ticket_pb2.Ticket) -> Ticket:
    """Creates a Ticket from a gRPC protobuf ticket.

    Args:
        ticket (ticket_pb2.Ticket): the gRPC protobuf ticket

    Returns:
        a Ticket

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
    elif ticket_bytes.startswith(b'a/'):
        return ApplicationTicket(ticket_bytes)
    else:
        raise DHError(f'Unknown ticket type: {ticket_bytes}')


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

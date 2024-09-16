#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from abc import ABC, abstractmethod
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


class ApplicationTicket(Ticket):
    """An ApplicationTicket is a ticket that references a field of an application on the server. """

    def __init__(self, ticket_bytes: bytes):
        """Initializes an ApplicationTicket object

        Args:
            ticket_bytes (bytes): the raw bytes for the ticket
        """
        if not ticket_bytes:
            raise DHError('ApplicationTicket: ticket is None')
        elif not ticket_bytes.startswith(b'a/'):
            raise DHError(f'ApplicationTicket: ticket {ticket_bytes} is not an application ticket')
        elif len(ticket_bytes.split(b'/')) != 3:
            raise DHError(f'ApplicationTicket: ticket {ticket_bytes} is not in the correct format')

        self.app_id = ticket_bytes.split(b'/')[1].decode(encoding='ascii')
        self.field = ticket_bytes.split(b'/')[2].decode(encoding='ascii')

        super().__init__(ticket_bytes)

    @classmethod
    def app_ticket(cls, app_id: str, field: str) -> ApplicationTicket:
        """Creates an application ticket that references a field of an application.

        Args:
            app_id (str): the application id
            field (str): the name of the application field

        Returns:
            an ApplicationTicket object
        """
        return cls(ticket_bytes=f'a/{app_id}/{field}'.encode(encoding='ascii'))


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
    elif ticket_bytes.startswith(b'a/'):
        return ApplicationTicket(ticket_bytes)
    else:
        raise DHError(f'Unknown ticket type: {ticket_bytes}')



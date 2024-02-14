#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

"""
Experimental module to communicate with server-side plugins from the client.
"""
import threading
from queue import SimpleQueue
from typing import Any, List, Union

from pydeephaven.proto import object_pb2
from pydeephaven.proto import ticket_pb2
from .server_object import ServerObject
from pydeephaven.dherror import DHError
from pydeephaven.table import Table


class PluginClient(ServerObject):
    """
    Connected to an object on the server, this provides access to that object through messages sent from the server.
    Use resp_stream to read messages that the server has sent, and req_stream to send messages back to the server, if
    supported.
    """

    def __init__(self, session: 'pydeephaven.session.Session', exportable_obj: ticket_pb2.Ticket):
        super().__init__(type_=exportable_obj.type, ticket=exportable_obj.ticket)
        self.session = session
        self.exportable_obj = exportable_obj
        self.req_stream = PluginRequestStream(SimpleQueue(), self.exportable_obj)
        self.resp_stream = PluginResponseStream(self._open(), self.session)

    def _open(self) -> Any:
        return self.session.plugin_object_service.message_stream(self.req_stream)

    def close(self) -> None:
        self.req_stream.close()
        self.resp_stream.close()


class Fetchable(ServerObject):
    """
    Represents an object on the server that could be fetched and used or communicated with from the client.
    """

    def __init__(self, session, typed_ticket: ticket_pb2.TypedTicket):
        super().__init__(type_=typed_ticket.type, ticket=typed_ticket.ticket)
        self.session = session
        self.typed_ticket = typed_ticket

    def fetch(self) -> Union[Table, PluginClient]:
        """
        Returns a client object that can be interacted with, representing an object that actually only exists on the
        server. In contrast to a Fetchable instance, which only serves as a reference to a server object,
        this method can return a Table or PluginClient. Note that closing this Fetchable or the result returned from
        this method will also close the other, take care when signaling that it is safe to release this object on
        the server.
        """
        if self.typed_ticket.type is None:
            raise DHError("Cannot fetch an object with no type, the server has no ObjectType plugin registered to "
                          "support it.")
        if self.typed_ticket.type == 'Table':
            return self.session.table_service.fetch_etcr(self.typed_ticket.ticket)
        return PluginClient(self.session, self.typed_ticket)

    def close(self) -> None:
        self.session.release(self.typed_ticket)


class PluginRequestStream:
    """
    A stream of requests to the server. If supported by the server-side plugin, these will be processed on the server
    in the order they are sent.
    """

    def __init__(self, req_queue: SimpleQueue, source_ticket):
        self.req_queue = req_queue
        connect_req = object_pb2.ConnectRequest(source_id=source_ticket)
        stream_req = object_pb2.StreamRequest(connect=connect_req)
        self.req_queue.put(stream_req)
        self._sentinel = object()

    def write(self, payload: bytes, references: List[ServerObject]) -> None:
        """
        Sends a message to the server, consisting of a payload of bytes and a list of objects that exist on the server.
        """
        data_message = object_pb2.ClientData(payload=payload, references=[obj.typed_ticket() for obj in references])
        stream_req = object_pb2.StreamRequest(data=data_message)
        self.req_queue.put(stream_req)

    def __next__(self):
        if (req := self.req_queue.get()) != self._sentinel:
            return req
        else:
            raise StopIteration

    def __iter__(self):
        return self

    def close(self) -> None:
        self.req_queue.put(self._sentinel)


class PluginResponseStream:
    """
    A stream of responses from the server. Will contain at least one response from when the object was first connected
    to, depending on the server implementation.
    """

    def __init__(self, stream_resp, session: 'pydeephaven.session.Session'):
        self.stream_resp = stream_resp
        self.session = session
        self._rlock = threading.RLock()

    def __next__(self):
        with self._rlock:
            if not self.stream_resp:
                raise RuntimeError("the response stream is closed.")
            try:
                resp = next(self.stream_resp)
            except StopIteration as e:
                raise
            else:
                return resp.data.payload, [Fetchable(self.session, ticket) for ticket in resp.data.exported_references]

    def __iter__(self):
        return self

    def close(self) -> None:
        with self._rlock:
            self.stream_resp = None

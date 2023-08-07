#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

#
#
from queue import SimpleQueue
from typing import Any, List

from pydeephaven.proto import object_pb2
from pydeephaven.proto import ticket_pb2
from pydeephaven import Session
from .server_object import ServerObject


class Fetchable(ServerObject):
    def __init__(self, session: Session, typed_ticket: ticket_pb2.TypedTicket):
        super().__init__(type_=typed_ticket.type, ticket=typed_ticket.ticket)
        self.session = session
        self.typed_ticket = typed_ticket

    def fetch(self):
        if (self.typed_ticket.type == 'Table'):
            # TODO we need to call TableService.ExportedTableCreationResponse
            return self.session.table_service #blah blah colin figure it out later
        return PluginClient(self.session, self.typed_ticket)

    def close(self):
        self.session.release(self.typed_ticket)


class PluginClient(ServerObject):
    def __init__(self, session, exportable_obj: ticket_pb2.TypedTicket):
        super().__init__(type_=exportable_obj.type, ticket=exportable_obj.ticket)
        self.session = session
        self.exportable_obj = exportable_obj
        self.req_stream = PluginRequestStream(SimpleQueue(), self.exportable_obj)
        self.resp_stream = PluginResponseStream(self._open(), self.session)

    def _open(self) -> Any:
        return self.session.plugin_object_service.message_stream(self.req_stream)

    def close(self):
        ...


class PluginRequestStream:
    def __init__(self, req_queue, source_ticket):
        self.req_queue = req_queue
        connect_req = object_pb2.ConnectRequest(source_id=source_ticket)
        stream_req = object_pb2.StreamRequest(connect=connect_req)
        self.req_queue.put(stream_req)

    def write(self, payload, references: List[ServerObject]):
        data_message = object_pb2.Data(payload=payload, exported_references=[obj.typed_ticket for obj in references])
        stream_req = object_pb2.StreamRequest(data=data_message)
        self.req_queue.put(stream_req)

    def __next__(self):
        return self.req_queue.get()

    def __iter__(self):
        return self


class PluginResponseStream:
    def __init__(self, stream_resp, session:Session):
        self.stream_resp = stream_resp
        self.session = session

    def __next__(self):
        resp = next(self.stream_resp)
        return resp.data.payload, [Fetchable(self.session, ticket) for ticket in resp.data.exported_references]

    def __iter__(self):
        return self

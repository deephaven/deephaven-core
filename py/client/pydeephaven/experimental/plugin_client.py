#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

#
#
from queue import SimpleQueue
from typing import Any

from pydeephaven.proto import object_pb2


class PluginClient:
    def __init__(self, session, exportable_obj):
        self.session = session
        self.exportable_obj = exportable_obj
        self.req_stream = PluginRequestStream(SimpleQueue(), self.exportable_obj.typed_ticket)
        self.resp_stream = PluginResponseStream(self._open())

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

    def write(self, payload, data):
        data_message = object_pb2.Data(payload=payload, data=data)
        stream_req = object_pb2.StreamRequest(data=data_message)
        self.req_queue.put(stream_req)

    def __next__(self):
        return self.req_queue.get()

    def __iter__(self):
        return self


class PluginResponseStream:
    def __init__(self, stream_resp):
        self.stream_resp = stream_resp

    def __next__(self):
        resp = next(self.stream_resp)
        return resp.data.payload, resp.data.exported_references

    def __iter__(self):
        return self

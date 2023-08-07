import abc
from abc import ABC
import dataclasses
from pydeephaven.proto import ticket_pb2


@dataclasses.dataclass
class ServerObject:
    type_: str
    ticket: bytes
    def typed_ticket(self) -> ticket_pb2.TypedTicket:
        return ticket_pb2.TypedTicket(type=self.type_, ticket=self.ticket)
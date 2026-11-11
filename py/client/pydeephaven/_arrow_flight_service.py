#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.flight as paflight
from pyarrow.flight import FlightCallOptions

from pydeephaven._arrow import decode_for_pyarrow, map_arrow_type, value_type_of
from pydeephaven.dherror import DHError
from pydeephaven.table import Table

if TYPE_CHECKING:
    from pydeephaven.session import Session


class ArrowFlightService:
    def __init__(self, session: Session, flight_client: paflight.FlightClient):
        self.session = session
        self._flight_client = flight_client

    def import_table(self, data: pa.Table) -> Table:
        """Uploads a pyarrow table into Deephaven via Flight do_put."""
        try:
            if not isinstance(data, pa.Table):
                raise DHError("source data must be a pyarrow Table.")
            ticket = self.session.next_export_ticket_number()
            dh_fields = []
            for f in data.schema:
                # Copy nullable from data. pa.field defaults to nullable=True, and the writer below rejects data
                # whose schema differs from dh_schema. data can have non-nullable fields, such as the REE fields
                # Table.to_arrow() returns.
                dh_fields.append(
                    pa.field(
                        name=f.name,
                        type=f.type,
                        nullable=f.nullable,
                        metadata=map_arrow_type(f.type),
                    )
                )
            dh_schema = pa.schema(dh_fields)

            writer, reader = self._flight_client.do_put(
                pa.flight.FlightDescriptor.for_path("export", str(ticket)),
                dh_schema,
                FlightCallOptions(headers=self.session.grpc_metadata),
            )
            writer.write_table(data)
            # Note that pyarrow's write_table completes the gRPC. If we send another gRPC close
            # it is possible that by the time the request arrives at the server that it no longer
            # knows what it is for and sends a RST_STREAM causing a failure.
            _ = reader.read()
            flight_ticket = self.session.make_export_ticket(ticket)
            # The server stores an encoded column as a plain column of its value type.
            table_schema = pa.schema(
                [
                    pa.field(f.name, value_type_of(f.type), True, f.metadata)
                    if value_type_of(f.type) != f.type
                    else f
                    for f in dh_schema
                ]
            )
            return Table(
                self.session,
                ticket=flight_ticket,
                size=data.num_rows,
                schema=table_schema,
            )
        except Exception as e:
            raise DHError("failed to create a Deephaven table from Arrow data.") from e

    def do_get_table(self, table: Table) -> pa.Table:
        """Gets a snapshot of a Table via Flight do_get."""
        try:
            flight_ticket = paflight.Ticket(table.ticket.bytes)
            reader = self._flight_client.do_get(
                flight_ticket, FlightCallOptions(headers=self.session.grpc_metadata)
            )

            return decode_for_pyarrow(reader.read_all())
        except Exception as e:
            raise DHError("failed to perform a flight DoGet on the table.") from e

    def do_exchange(
        self,
    ) -> tuple[paflight.FlightStreamWriter, paflight.FlightStreamReader]:
        """Starts an Arrow do_exchange operation.

        Returns:
            The corresponding Arrow FlightStreamWriter and FlightStreamReader.
        """
        try:
            desc = pa.flight.FlightDescriptor.for_command(b"dphn")
            writer, reader = self._flight_client.do_exchange(
                desc, FlightCallOptions(headers=self.session.grpc_metadata)
            )
            return writer, reader

        except Exception as e:
            raise DHError("failed to perform a flight DoExchange on the table.") from e

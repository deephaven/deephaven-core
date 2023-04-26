#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import pyarrow as pa
import pyarrow.flight as paflight

from pydeephaven._arrow import map_arrow_type
from pydeephaven.dherror import DHError
from pydeephaven.table import Table


class ArrowFlightService:
    def __init__(self, session, flight_client):
        self.session = session
        self._flight_client = flight_client

    def import_table(self, data: pa.Table) -> Table:
        """Uploads a pyarrow table into Deephaven via. Flight do_put."""
        try:
            options = paflight.FlightCallOptions(headers=self.session.grpc_metadata)
            if not isinstance(data, (pa.Table, pa.RecordBatch)):
                raise DHError("source data must be either a pa table or RecordBatch.")
            ticket = self.session.get_ticket()
            dh_fields = []
            for f in data.schema:
                dh_fields.append(pa.field(name=f.name, type=f.type, metadata=map_arrow_type(f.type)))
            dh_schema = pa.schema(dh_fields)

            writer, reader = self._flight_client.do_put(
                pa.flight.FlightDescriptor.for_path("export", str(ticket)), dh_schema, options=options)
            writer.write_table(data)
            writer.close()
            _ = reader.read()
            flight_ticket = self.session.make_ticket(ticket)
            return Table(self.session, ticket=flight_ticket, size=data.num_rows, schema=dh_schema)
        except Exception as e:
            raise DHError("failed to create a Deephaven table from Arrow data.") from e

    def do_get_table(self, table: Table) -> pa.Table:
        """Gets a snapshot of a Table via. Flight do_get."""
        try:
            options = paflight.FlightCallOptions(headers=self.session.grpc_metadata)
            flight_ticket = paflight.Ticket(table.ticket.ticket)
            reader = self._flight_client.do_get(flight_ticket, options=options)
            return reader.read_all()
        except Exception as e:
            raise DHError("failed to perform a flight DoGet on the table.") from e

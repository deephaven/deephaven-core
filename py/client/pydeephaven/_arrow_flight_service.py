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
        """Uploads a pyarrow table into Deephaven via Flight do_put."""
        try:
            if not isinstance(data, (pa.Table, pa.RecordBatch)):
                raise DHError("source data must be either a pa table or RecordBatch.")
            ticket = self.session.get_ticket()
            dh_fields = []
            for f in data.schema:
                dh_fields.append(pa.field(name=f.name, type=f.type, metadata=map_arrow_type(f.type)))
            dh_schema = pa.schema(dh_fields)

            # No need to add headers/metadata here via the options argument;
            # or middleware is already doing it for every call.
            writer, reader = self._flight_client.do_put(
                pa.flight.FlightDescriptor.for_path("export", str(ticket)), dh_schema)
            writer.write_table(data)
            # Note that pyarrow's write_table completes the gRPC. If we send another gRPC close
            # it is possible that by the time the request arrives at the server that it no longer
            # knows what it is for and sends a RST_STREAM causing a failure.
            _ = reader.read()
            flight_ticket = self.session.make_ticket(ticket)
            return Table(self.session, ticket=flight_ticket, size=data.num_rows, schema=dh_schema)
        except Exception as e:
            raise DHError("failed to create a Deephaven table from Arrow data.") from e

    def do_get_table(self, table: Table) -> pa.Table:
        """Gets a snapshot of a Table via Flight do_get."""
        try:
            flight_ticket = paflight.Ticket(table.ticket.ticket)
            # No need to add headers/metadata here via the options argument;
            # or middleware is already doing it for every call.
            reader = self._flight_client.do_get(flight_ticket)
            return reader.read_all()
        except Exception as e:
            raise DHError("failed to perform a flight DoGet on the table.") from e

    def do_exchange(self):
        """Starts an Arrow do_exchange operation.

        Returns:
            The corresponding Arrow FlightStreamWriter and FlightStreamReader.
        """
        try:
            desc = pa.flight.FlightDescriptor.for_command(b"dphn")
            options = paflight.FlightCallOptions(headers=self.session.grpc_metadata)
            writer, reader = self._flight_client.do_exchange(desc, options)
            return writer, reader

        except Exception as e:
            raise DHError("failed to perform a flight DoExchange on the table.") from e


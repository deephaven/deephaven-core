#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import pyarrow
import pyarrow as pa
import pyarrow.flight as paflight
from pydeephaven.dherror import DHError
from pydeephaven.table import Table


def _map_arrow_type(arrow_type):
    arrow_to_dh = {
        pa.null(): '',
        pa.bool_(): '',
        pa.int8(): 'byte',
        pa.int16(): 'short',
        pa.int32(): 'int',
        pa.int64(): 'long',
        pa.uint8(): '',
        pa.uint16(): 'char',
        pa.uint32(): '',
        pa.uint64(): '',
        pa.float16(): '',
        pa.float32(): 'float',
        pa.float64(): 'double',
        pa.time32('s'): '',
        pa.time32('ms'): '',
        pa.time64('us'): '',
        pa.time64('ns'): 'io.deephaven.time.DateTime',
        pa.timestamp('us', tz=None): '',
        pa.timestamp('ns', tz=None): '',
        pa.date32(): 'java.time.LocalDate',
        pa.date64(): 'java.time.LocalDate',
        pa.binary(): '',
        pa.string(): 'java.lang.String',
        pa.utf8(): 'java.lang.String',
        pa.large_binary(): '',
        pa.large_string(): '',
        pa.large_utf8(): '',
        # decimal128(int precision, int scale=0)
        # list_(value_type, int list_size=-1)
        # large_list(value_type)
        # map_(key_type, item_type[, keys_sorted])
        # struct(fields)
        # dictionary(index_type, value_type, …)
        # field(name, type, bool nullable = True[, metadata])
        # schema(fields[, metadata])
        # from_numpy_dtype(dtype)
    }

    dh_type = arrow_to_dh.get(arrow_type)
    if not dh_type:
        # if this is a case of timestamp with tz specified
        if isinstance(arrow_type, pa.TimestampType):
            dh_type = "io.deephaven.time.DateTime"

    if not dh_type:
        raise DHError(f'unsupported arrow data type : {arrow_type}')

    return {"deephaven:type": dh_type}


class ArrowFlightService:
    def __init__(self, session):
        self.session = session
        self._flight_client = paflight.connect((session.host, session.port))

    def import_table(self, data:pyarrow.Table):
        try:
            options = paflight.FlightCallOptions(headers=self.session.grpc_metadata)
            if not isinstance(data, (pa.Table, pa.RecordBatch)):
                raise DHError("source data must be either a pa table or RecordBatch.")
            ticket = self.session.get_ticket()
            dh_fields = []
            for f in data.schema:
                dh_fields.append(pa.field(name=f.name, type=f.type, metadata=_map_arrow_type(f.type)))
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

    def snapshot_table(self, table:Table):
        try:
            options = paflight.FlightCallOptions(headers=self.session.grpc_metadata)
            flight_ticket = paflight.Ticket(table.ticket.ticket)
            reader = self._flight_client.do_get(flight_ticket, options=options)
            return reader.read_all()
        except Exception as e:
            raise DHError("failed to take a snapshot of the table.") from e

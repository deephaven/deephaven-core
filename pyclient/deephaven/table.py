import pyarrow

from deephaven.dherror import DHError


class Table:
    def __init__(self, session=None, ticket=None, schema_header=b'', size=0, is_static=True, schema=None):
        if not session or not session.is_alive:
            raise DHError("Must be associated with a active session")
        self.session = session
        self.ticket = ticket
        self.schema = schema
        self.is_static = is_static
        self.size = size
        self.cols = []
        if not schema:
            self._parse_schema(schema_header)

    def update(self, column_specs=[]):
        return self.session.update_table(self, column_specs)

    def _parse_schema(self, schema_header):
        if not schema_header:
            return

        reader = pyarrow.ipc.open_stream(schema_header)
        self.schema = reader.schema

        # from barrage.flatbuf.Schema import Schema
        # schema = Schema.GetRootAs(self.schema_header)
        # for i in range(schema.FieldsLength()):
        #     field_attrs = {}
        #     field = schema.Fields(i)
        #     print(dir(field))
        #     field_attrs['nullable'] = field.Nullable()
        #     field_attrs['type'] = field.Type()
        #     field_attrs['type.type'] = field.TypeType()
        #     for j in range(field.CustomMetadataLength()):
        #         kv = field.CustomMetadata(j)
        #         field_attrs[kv.Key()] = kv.Value()
        #     self.cols.append(Column(field.Name(), field_attrs))
        #     # print(field)
        #     # print("")

    def snapshot(self) -> pyarrow.Table:
        # self.session.subscribe_table(self)
        return self.session.snapshot_table(self)

    def filter(self):
        ...


class EmptyTable(Table):
    def __init__(self, session=None, ticket=None, schema_header=b'', size=0, is_static=True):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header,
                         size=size)


class TimeTable(Table):
    def __init__(self, session=None, ticket=None, schema_header=b'', start_time=None, period=1000000000,
                 is_static=False):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header,
                         is_static=is_static)
        self.start_time = start_time
        self.period = period

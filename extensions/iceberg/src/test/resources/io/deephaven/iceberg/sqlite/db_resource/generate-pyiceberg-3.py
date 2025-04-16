'''
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from datetime import datetime, date, time
from decimal import Decimal
import numpy as np
import zoneinfo
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    TimestamptzType,
    TimeType,
    DateType,
    DecimalType,
    BinaryType,
    StringType,
    BooleanType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    ListType,
    NestedField
)

catalog = SqlCatalog(
    "pyiceberg-3",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-3",
    },
)

# Number of bytes for fixed-length binary
fixed_size = 13

schema = Schema(
    NestedField(field_id=1, name="bin_col",
                field_type=BinaryType(), required=False), # variable-length binary
    NestedField(field_id=2, name="fixed_col",
                field_type=FixedType(fixed_size), required=False), # fixed-length binary
    NestedField(field_id=3, name="long_list",
                field_type=ListType(element_id=4, element=LongType(), element_required=False), required=False),
    NestedField(field_id=5, name="bool_list",
                field_type=ListType(element_id=6, element=BooleanType(), element_required=False), required=False),
    NestedField(field_id=7, name="double_list",
                field_type=ListType(element_id=8, element=DoubleType(), element_required=False), required=False),
    NestedField(field_id=9, name="float_list",
                field_type=ListType(element_id=10, element=FloatType(), element_required=False), required=False),
    NestedField(field_id=11, name="int_list",
                field_type=ListType(element_id=12, element=IntegerType(), element_required=False), required=False),
    NestedField(field_id=13, name="string_list",
                field_type=ListType(element_id=14, element=StringType(), element_required=False), required=False),
    NestedField(field_id=15, name="timestamp_ntz_list",
                field_type=ListType(element_id=16, element=TimestampType(), element_required=False), required=False),
    NestedField(field_id=17, name="timestamp_tz_list",
                field_type=ListType(element_id=18, element=TimestamptzType(), element_required=False), required=False),
    NestedField(field_id=19, name="date_list",
                field_type=ListType(element_id=20, element=DateType(), element_required=False), required=False),
    NestedField(field_id=21, name="time_list",
                field_type=ListType(element_id=22, element=TimeType(), element_required=False), required=False),
    NestedField(field_id=23, name="decimal_list",
                field_type=ListType(element_id=24, element=DecimalType(10, 2), element_required=False), required=False),
)

catalog.create_namespace_if_not_exists("list_test")

table_identifier = "list_test.data"
if catalog.table_exists(table_identifier):
    catalog.purge_table(table_identifier)

tbl = catalog.create_table(
    identifier=table_identifier,
    schema=schema,
)

data = [
    {
        "bin_col": b"variable length data",
        "fixed_col": b"123456789ABCD",
        "long_list": [100, 200, 300],
        "bool_list": [True, False],
        "double_list": [10.01, 20.02],
        "float_list": [np.float32(1.1), np.float32(2.2)],
        "int_list": [np.int32(10), np.int32(20)],
        "string_list": ["hello", "world"],
        "timestamp_ntz_list": [datetime(2025, 1, 1, 12, 0, 1), datetime(2025, 1, 1, 12, 0, 2)],
        "timestamp_tz_list": [
            datetime(2025, 1, 1, 12, 0, 3, tzinfo=zoneinfo.ZoneInfo("UTC")),
            datetime(2025, 1, 1, 12, 0, 4, tzinfo=zoneinfo.ZoneInfo("UTC")),
        ],
        "date_list": [date(2025, 1, 1), date(2025, 1, 2)],
        "time_list": [time(12, 0, 1), time(13, 0, 2)],
        "decimal_list": [Decimal("123.45"), Decimal("678.90")],
    },
    {
        "bin_col": b"",
        "fixed_col": b"13 bytes only",
        "long_list": [400, 500],
        "bool_list": [False, True],
        "double_list": [30.03, 40.04],
        "float_list": [np.float32(3.3), np.float32(4.4)],
        "int_list": [np.int32(30), np.int32(40)],
        "string_list": ["foo", "bar", "baz"],
        "timestamp_ntz_list": [datetime(2025, 1, 2, 13, 0, 1), datetime(2025, 1, 2, 13, 0, 2)],
        "timestamp_tz_list": [
            datetime(2025, 1, 2, 13, 0, 3, tzinfo=zoneinfo.ZoneInfo("UTC")),
            datetime(2025, 1, 2, 13, 0, 4, tzinfo=zoneinfo.ZoneInfo("UTC")),
        ],
        "date_list": [date(2025, 1, 3), date(2025, 1, 4)],
        "time_list": [time(14, 0, 3), time(15, 0, 4)],
        "decimal_list": [Decimal("234.56"), Decimal("987.65")],
    },
]

arrow_schema = pa.schema([
    pa.field("bin_col", pa.binary()),
    pa.field("fixed_col", pa.binary(fixed_size)),  # explicitly defined as fixed-size binary of right size
    pa.field("long_list", pa.list_(pa.int64())),
    pa.field("bool_list", pa.list_(pa.bool_())),
    pa.field("double_list", pa.list_(pa.float64())),
    pa.field("float_list", pa.list_(pa.float32())),
    pa.field("int_list", pa.list_(pa.int32())),
    pa.field("string_list", pa.list_(pa.string())),
    pa.field("timestamp_ntz_list", pa.list_(pa.timestamp("us"))),
    pa.field("timestamp_tz_list", pa.list_(pa.timestamp("us", tz="UTC"))),
    pa.field("date_list", pa.list_(pa.date32())),
    pa.field("time_list", pa.list_(pa.time64("us"))),
    pa.field("decimal_list", pa.list_(pa.decimal128(10, 2))),
])

# Create a PyArrow Table
table = pa.Table.from_pylist(data, schema=arrow_schema)

# Append the table to the Iceberg table
tbl.append(table)


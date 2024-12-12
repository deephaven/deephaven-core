'''
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, FloatType, DoubleType, StringType, NestedField, StructType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

catalog = SqlCatalog(
    "pyiceberg-2",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-2",
    },
)

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=3, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
)

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day",
    ),
    PartitionField(
        source_id=2, field_id=1001, transform=IdentityTransform(), name="symbol",
    )
)

catalog.create_namespace("trading")

tbl = catalog.create_table(
    identifier="trading.data",
    schema=schema,
    partition_spec=partition_spec,
)

# Define the data according to your Iceberg schema
data = [
    {"datetime": datetime(2024, 11, 27, 10, 0, 0), "symbol": "AAPL", "bid": 150.25, "ask": 151.0},
    {"datetime": datetime(2024, 11, 27, 10, 0, 0), "symbol": "MSFT", "bid": 150.25, "ask": 151.0},
    {"datetime": datetime(2024, 11, 26, 10, 1, 0), "symbol": "GOOG", "bid": 2800.75, "ask": 2810.5},
    {"datetime": datetime(2024, 11, 26, 10, 2, 0), "symbol": "AMZN", "bid": 3400.5, "ask": 3420.0},
    {"datetime": datetime(2024, 11, 28, 10, 3, 0), "symbol": "MSFT", "bid": None, "ask": 250.0},
]

# Create a PyArrow Table
table = pa.Table.from_pylist(data)

# Append the table to the Iceberg table
tbl.append(table)

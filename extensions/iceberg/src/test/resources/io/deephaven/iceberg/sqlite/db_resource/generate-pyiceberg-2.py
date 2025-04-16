'''
This script uses PyIceberg to create an Iceberg table with a partition spec containing a non-identity transform field.
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, FloatType, DoubleType, StringType, NestedField, StructType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

import pyiceberg_test_utils

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

catalog.create_namespace_if_not_exists("trading")

table_identifier = "trading.data"
tbl = pyiceberg_test_utils.create_table_purging_if_exists(catalog, table_identifier, schema, partition_spec)

# Append the table to the Iceberg table
tbl.append(pyiceberg_test_utils.MERGED_DATASET)

######## Empty table testing ########
table_identifier = "trading.data_empty"
tbl = pyiceberg_test_utils.create_table_purging_if_exists(catalog, table_identifier, schema, partition_spec)

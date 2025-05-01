'''
This script uses PyIceberg to create an Iceberg table where we add new partition fields.
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, FloatType, DoubleType, StringType, NestedField, StructType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, IdentityTransform

import pyiceberg_test_utils

catalog = SqlCatalog(
    "pyiceberg-3",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-3",
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
        source_id=2, field_id=1001, transform=IdentityTransform(), name="symbol",
    )
)

catalog.create_namespace_if_not_exists("trading")

def add_non_identity_partition_field():
    table_identifier = "trading.add_non_identity_partition_field"
    tbl = pyiceberg_test_utils.create_table_purging_if_exists(catalog, table_identifier, schema, partition_spec)

    tbl.append(pyiceberg_test_utils.DATASET_1)

    # Add a non-identity partition field
    with tbl.update_spec() as update:
        update.add_field("datetime", YearTransform(), "datetime_year")

    tbl.append(pyiceberg_test_utils.DATASET_2)

def add_identity_partition_field():
    table_identifier = "trading.add_identity_partition_field"
    tbl = pyiceberg_test_utils.create_table_purging_if_exists(catalog, table_identifier, schema, partition_spec)

    tbl.append(pyiceberg_test_utils.DATASET_1)

    # Add an identity partition field
    with tbl.update_spec() as update:
        update.add_field("bid", IdentityTransform())

    tbl.append(pyiceberg_test_utils.DATASET_2)

add_non_identity_partition_field()
add_identity_partition_field()

'''
This script uses PyIceberg to create an Iceberg table where we drop and reorder partition fields.
See TESTING.md for how to run this script.
'''

import pyarrow as pa
from datetime import datetime
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, FloatType, DoubleType, StringType, NestedField, StructType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, IdentityTransform

from pyiceberg_test_utils import DATASET_1, DATASET_2

catalog = SqlCatalog(
    "pyiceberg-5",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-5",
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
    ),
    PartitionField(
        source_id=3, field_id=1000, transform=IdentityTransform(), name="bid",
    )
)

catalog.create_namespace_if_not_exists("trading")

def drop_identity_partition_field():
    table_identifier = "trading.drop_identity_partition_field"
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)

    tbl = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(DATASET_1)

    with tbl.update_spec() as update:
        update.remove_field("symbol")

    tbl.append(DATASET_2)

def rename_partition_field():
    table_identifier = "trading.reorder_partition_field"
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)

    tbl = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(DATASET_1)

    with tbl.update_spec() as update:
        update.rename_field("symbol", "sym")

    tbl.append(DATASET_2)

drop_identity_partition_field()
rename_partition_field()
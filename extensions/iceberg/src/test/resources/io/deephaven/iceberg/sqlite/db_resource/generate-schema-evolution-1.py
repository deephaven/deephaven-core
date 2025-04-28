"""
See TESTING.md for how to run this script.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PYARROW_PARQUET_FIELD_ID_KEY
from pyiceberg.table.update.schema import UpdateSchema

import pyarrow as pa

import iceberg_utils

TABLE_ID = ("schema-evolution", "table-1")
FIELD_ID_1 = 1
FIELD_ID_2 = 2
FIELD_ID_3 = 3

SCHEMA_INIT = Schema(
    NestedField(field_id=-1, name="Field1", field_type=IntegerType()),
    NestedField(field_id=-1, name="Field2", field_type=IntegerType()),
)

# Technically, the assigning of field ids is up to the Catalog implementation,
# and we should be forced to create the Table first before we actually know what
SCHEMA_0 = Schema(
    NestedField(field_id=FIELD_ID_1, name="Field1", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_2, name="Field2", field_type=IntegerType()),
)

# The rest of these schema modifications are renames, or re-orders, where we can
# know the specific field ids.

SCHEMA_1 = Schema(
    NestedField(field_id=FIELD_ID_1, name="Field1_B", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_2, name="Field2_B", field_type=IntegerType()),
)

SCHEMA_2 = Schema(
    NestedField(field_id=FIELD_ID_1, name="Field1_C", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_2, name="Field2_C", field_type=IntegerType()),
)

SCHEMA_3 = Schema(
    NestedField(field_id=FIELD_ID_2, name="Field2_C", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_1, name="Field1_C", field_type=IntegerType()),
)

SCHEMA_4 = Schema(
    NestedField(field_id=FIELD_ID_1, name="Field1_D", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_2, name="Field2_D", field_type=IntegerType()),
    NestedField(field_id=FIELD_ID_3, name="Field3_D", field_type=IntegerType()),
)


class PyArrowTest1:
    # By specifying the field ids in this way, we can pass off tables to pyiceberg without
    # needing to worry about the Iceberg table's current schema naming convention. It is a
    # _little_ weird that it's scoped as a "PARQUET" concept as oppposed to an "ICEBERG"
    # concept. https://github.com/apache/iceberg-python/pull/227
    SCHEMA = pa.schema([
        pa.field(
            name="ArrowField1",
            type=pa.int32(),
            metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(FIELD_ID_1)},
        ),
        pa.field(
            name="ArrowField2",
            type=pa.int32(),
            metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(FIELD_ID_2)},
        ),
    ])

    @staticmethod
    def table(rng: range) -> pa.Table:
        field_1_array = pa.array(rng, type=pa.int32())
        field_2_array = pa.array([-i for i in rng], type=pa.int32())
        return pa.table([field_1_array, field_2_array], schema=PyArrowTest1.SCHEMA)


catalog = SqlCatalog(
    "schema-evolution",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/schema-evolution",
    },
)

with catalog.create_table_transaction(TABLE_ID, SCHEMA_INIT) as txn:
    txn.append(PyArrowTest1.table(range(10)))

iceberg_table = catalog.load_table(TABLE_ID)
if iceberg_table.schema() != SCHEMA_0:
    raise Exception("Unexpected schema")

with iceberg_table.transaction() as txn:
    with txn.update_schema() as update_schema:
        iceberg_utils.do_update(update_schema, SCHEMA_1)

iceberg_table.append(PyArrowTest1.table(range(10, 20)))

with iceberg_table.transaction() as txn:
    txn.append(PyArrowTest1.table(range(20, 30)))
    with txn.update_schema() as update_schema:
        iceberg_utils.do_update(update_schema, SCHEMA_2)
    txn.append(PyArrowTest1.table(range(30, 40)))


with iceberg_table.transaction() as txn:
    txn.append(PyArrowTest1.table(range(40, 50)))
    with txn.update_schema() as update_schema:
        update_schema.move_first(SCHEMA_3.find_field(FIELD_ID_2).name)
        iceberg_utils._verify(update_schema, SCHEMA_3)
    txn.append(PyArrowTest1.table(range(50, 60)))

with iceberg_table.transaction() as txn:
    with txn.update_schema() as update_schema:
        update_schema.move_first(SCHEMA_3.find_field(FIELD_ID_1).name)
        update_schema.rename_column(
            SCHEMA_3.find_field(FIELD_ID_1).name, SCHEMA_4.find_field(FIELD_ID_1).name
        )
        update_schema.rename_column(
            SCHEMA_3.find_field(FIELD_ID_2).name, SCHEMA_4.find_field(FIELD_ID_2).name
        )

        f3 = SCHEMA_4.find_field(FIELD_ID_3)
        update_schema.add_column(f3.name, f3.field_type, f3.doc, f3.required)

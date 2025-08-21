"""
See TESTING.md for how to run this script.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StructType
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PYARROW_PARQUET_FIELD_ID_KEY

import pyarrow as pa

TABLE_ID = ("schema-evolution", "nested-rename")

# Deephaven does not support multiple optional nested levels, so we'll just make either the outer or the inner fields required
SCHEMA_INIT = Schema(
    NestedField(
        field_id=-1,
        name="Foo",
        field_type=StructType(
            NestedField(field_id=-1, name="Field1", field_type=IntegerType()),
            NestedField(field_id=-1, name="Field2", field_type=IntegerType()),
        ),
        required=True,
    ),
    NestedField(
        field_id=-1,
        name="Bar",
        field_type=StructType(
            NestedField(required=True, field_id=-1, name="Field1", field_type=IntegerType()),
            NestedField(required=True, field_id=-1, name="Field2", field_type=IntegerType()),
        ),
    ),
)

catalog = SqlCatalog(
    "schema-evolution",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/schema-evolution",
    },
)

#catalog.drop_table(TABLE_ID)
iceberg_table = catalog.create_table(TABLE_ID, SCHEMA_INIT)

FOO = iceberg_table.schema().find_field("Foo")
BAR = iceberg_table.schema().find_field("Bar")
# Some sort of pydantic runtime error stops this from running...
# we'll just use python duck-typing and assume it's a StructType
# FOO_F1 = (StructType)(FOO.field_type).field_by_name("Field1")
# FOO_F2 = (StructType)(FOO.field_type).field_by_name("Field2")
# BAR_F1 = (StructType)(BAR.field_type).field_by_name("Field1")
# BAR_F2 = (StructType)(BAR.field_type).field_by_name("Field2")
FOO_F1 = FOO.field_type.field_by_name("Field1")
FOO_F2 = FOO.field_type.field_by_name("Field2")
BAR_F1 = BAR.field_type.field_by_name("Field1")
BAR_F2 = BAR.field_type.field_by_name("Field2")


class PyArrowTest2:
    _FOO_NAME = "ArrowFoo"
    _BAR_NAME = "ArrowBar"
    _F1_NAME = "ArrowField1"
    _F2_NAME = "ArrowField2"

    # By specifying the field ids in this way, we can pass off tables to pyiceberg without
    # needing to worry about the Iceberg table's current schema naming convention. It is a
    # _little_ weird that it's scoped as a "PARQUET" concept as oppposed to an "ICEBERG"
    # concept. https://github.com/apache/iceberg-python/pull/227
    SCHEMA = pa.schema([
        pa.field(
            name=_FOO_NAME,
            type=pa.struct([
                pa.field(
                    name=_F1_NAME,
                    type=pa.int32(),
                    metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(FOO_F1.field_id)},
                ),
                pa.field(
                    name=_F2_NAME,
                    type=pa.int32(),
                    metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(FOO_F2.field_id)},
                ),
            ]),
            metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(FOO.field_id)},
            nullable=False,
        ),
        pa.field(
            name=_BAR_NAME,
            type=pa.struct([
                pa.field(
                    name=_F1_NAME,
                    type=pa.int32(),
                    metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(BAR_F1.field_id)},
                    nullable=False,
                ),
                pa.field(
                    name=_F2_NAME,
                    type=pa.int32(),
                    metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(BAR_F2.field_id)},
                    nullable=False,
                ),
            ]),
            metadata={PYARROW_PARQUET_FIELD_ID_KEY: str(BAR.field_id)},
        ),
    ])

    @staticmethod
    def table(rng: range) -> pa.Table:
        # TODO: not sure how to construct this efficiently with pyarrow arrays (like we can with the non-nested case)
        return pa.table(
            {
                PyArrowTest2._FOO_NAME: [
                    {PyArrowTest2._F1_NAME: i, PyArrowTest2._F2_NAME: -i} for i in rng
                ],
                PyArrowTest2._BAR_NAME: [
                    {PyArrowTest2._F1_NAME: i, PyArrowTest2._F2_NAME: -i} for i in rng
                ],
            },
            schema=PyArrowTest2.SCHEMA,
        )


iceberg_table = catalog.load_table(TABLE_ID)

iceberg_table.append(PyArrowTest2.table(range(0, 5)))

with iceberg_table.transaction() as txn:
    with txn.update_schema() as update_schema:
        # Rename top-level field
        update_schema.rename_column("Foo", "Foo_B")

        # Rename inner-fields
        update_schema.rename_column(("Bar", "Field1"), "Field1_B")
        update_schema.rename_column(("Bar", "Field2"), "Field2_B")

iceberg_table.append(PyArrowTest2.table(range(5, 10)))

with iceberg_table.transaction() as txn:
    with txn.update_schema() as update_schema:
        # Rename both levels
        update_schema.rename_column("Foo_B", "Foo_C")
        update_schema.rename_column(("Foo_B", "Field1"), "Field1_C")
        update_schema.rename_column(("Foo_B", "Field2"), "Field2_C")

iceberg_table.append(PyArrowTest2.table(range(10, 15)))

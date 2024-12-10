'''
See TESTING.md for how to run this script.
'''

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType
from pyiceberg.catalog.sql import SqlCatalog

import pyarrow as pa

catalog = SqlCatalog(
    "pyiceberg-1",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/pyiceberg-1",
    },
)

original_schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "latitude", DoubleType(), required=False),
    NestedField(3, "lon", DoubleType(), required=False),
)

#  Using specific names to make clear these aren't a standard / convention
catalog.create_namespace("dh-default")

table = catalog.create_table(
    "dh-default.cities",
    schema=original_schema,
)

# Add some data
table.append(
    pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "latitude": 52.371807, "lon": 4.896029},
            {"city": "San Francisco", "latitude": 37.773972, "lon": -122.431297},
            {"city": "Drachten", "latitude": 53.11254, "lon": 6.0989},
            {"city": "Paris", "latitude": 48.864716, "lon": 2.349014},
        ],
    )
)

# Oops, we should be consistent with naming
with table.update_schema() as update:
    update.rename_column("lon", "longitude")

# Add some data. Note, to simplify ingestion, we are matching the latest column names
table.append(
    pa.Table.from_pylist(
        [
            {"city": "Minneapolis", "latitude": 44.977479, "longitude": -93.264358},
            {"city": "New York", "latitude": 40.730610, "longitude": -73.935242},
        ],
    )
)

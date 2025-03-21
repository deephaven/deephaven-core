"""
See TESTING.md for how to run this script.
"""

from pyiceberg.catalog.sql import SqlCatalog


catalog = SqlCatalog(
    "schema-evolution",
    **{
        "uri": f"sqlite:///dh-iceberg-test.db",
        "warehouse": f"catalogs/schema-evolution",
    },
)
catalog.create_namespace("schema-evolution")

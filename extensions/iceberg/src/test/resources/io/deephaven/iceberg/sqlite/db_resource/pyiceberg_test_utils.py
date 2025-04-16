'''
This script contains sample data and helper methods used for testing Iceberg tables.
'''
import pyarrow as pa
from datetime import datetime

DATASET_1 = pa.Table.from_pylist([
    {"datetime": datetime(2024, 11, 27, 10, 0, 0), "symbol": "AAPL", "bid": 150.25, "ask": 151.0},
    {"datetime": datetime(2022, 11, 27, 10, 0, 0), "symbol": "MSFT", "bid": 150.25, "ask": 151.0},
])

DATASET_2 = pa.Table.from_pylist([
    {"datetime": datetime(2022, 11, 26, 10, 1, 0), "symbol": "GOOG", "bid": 2800.75, "ask": 2810.5},
    {"datetime": datetime(2023, 11, 26, 10, 2, 0), "symbol": "AMZN", "bid": 3400.5, "ask": 3420.0},
    {"datetime": datetime(2025, 11, 28, 10, 3, 0), "symbol": "MSFT", "bid": 238.85, "ask": 250.0},
])

MERGED_DATASET = pa.concat_tables([DATASET_1, DATASET_2])

def create_table_purging_if_exists(catalog, table_identifier, schema, partition_spec = None):
    if catalog.table_exists(table_identifier):
        catalog.purge_table(table_identifier)

    if partition_spec is None:
        return catalog.create_table(
            identifier=table_identifier,
            schema=schema)

    return catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec)

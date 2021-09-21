#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" Demo how to merge tables in Deephaven."""
import pandas as pd

from examples.import_test_data import import_taxi_records
from examples.demo_query import demo_query
from examples.demo_table_ops import demo_chained_table_ops
from pydeephaven import Session


def main():
    with Session(host="localhost", port=10000) as dh_session:
        taxi_data_table = import_taxi_records(dh_session)

        top_5_fares_table = demo_query(dh_session=dh_session, taxi_data_table=taxi_data_table)
        bottom_5_fares_table = demo_chained_table_ops(taxi_data_table)

        combined_fares_table = dh_session.merge_tables(tables=[top_5_fares_table, bottom_5_fares_table])
        snapshot_data = combined_fares_table.snapshot()
        df = snapshot_data.to_pandas()

        pd.set_option("max_columns", 20)
        print(df)


if __name__ == '__main__':
    main()

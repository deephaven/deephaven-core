#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""Demo how to build and run a query on a Deephaven server."""
import pandas as pd

from examples.import_test_data import import_taxi_records
from pydeephaven import Session, Table


def demo_query(dh_session: Session, taxi_data_table: Table) -> Table:
    # create a query and execute it on the DH server
    query = (dh_session.query(taxi_data_table)
             .where(filters=["VendorID > 0"])
             .sort(order_by=["VendorID", "fare_amount"])
             .tail_by(num_rows=5, by=["VendorID"]))
    return query.exec()


def main():
    with Session(host="localhost", port=10000) as dh_session:
        taxi_data_table = import_taxi_records(dh_session)

        top_5_fares_table = demo_query(dh_session=dh_session, taxi_data_table=taxi_data_table)

        # download the table to the client in the form of pyarrow table and convert it into a Pandas DataFrame
        snapshot_data = top_5_fares_table.snapshot()
        df = snapshot_data.to_pandas()

        pd.set_option("max_columns", 20)
        print(df)


if __name__ == '__main__':
    main()

#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Demo the steps to upload CSV data into Deephaven

    1. use pyarrow to load external data of interest into memory
    2. prepare the data
    3. import the data into the Deephaven server

"""

from pyarrow import csv

from examples.csv_downloader import download_csv
from pydeephaven import Session, Table
from pydeephaven.utils import is_deephaven_compatible


def import_taxi_records(dh_session: Session) -> Table:

    # download the CSV data and read it into a pyarrow table and prepare it for uploading into DH
    csv_file_name = download_csv(url="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2020-12.csv")
    pa_table = csv.read_csv(csv_file_name)

    # drop unwanted columns
    unwanted_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "RatecodeID",
                        "store_and_fwd_flag", "PULocationID", "DOLocationID"]
    pa_table = pa_table.drop(unwanted_columns)

    # drop any column with a unsupported data type
    for column, column_name in zip(pa_table.columns, pa_table.column_names):
        if not is_deephaven_compatible(column.type):
            print(f"drop column: {column_name} because of unsupported data type {column.type}")
            pa_table = pa_table.drop([column_name])

    # upload the pyarrow table to the Deephaven server
    return dh_session.import_table(pa_table)
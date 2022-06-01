#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" Demo the steps to upload Parquet data into Deephaven
    1. use pyarrow to load external data of interest into memory
    2. prepare the data
    3. import the data into the Deephaven server
"""

from pyarrow import parquet

from examples.file_downloader import download_file
from pydeephaven import Session, Table
from pydeephaven.utils import is_deephaven_compatible


def import_taxi_records(dh_session: Session) -> Table:

    # download the data and read it into a pyarrow table and prepare it for uploading into DH
    file_name = download_file(url="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2020-12.parquet", file_name="sample.parquet")
    pa_table = parquet.read_table(file_name)

    # drop unwanted columns
    unwanted_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "RatecodeID",
                        "store_and_fwd_flag", "PULocationID", "DOLocationID"]
    pa_table = pa_table.drop(unwanted_columns)

    # this is necessary because otherwise the data is too big to be sent
    new_len = pa_table.num_rows // 2
    pa_table = pa_table.slice(length=new_len)

    # drop any column with a unsupported data type
    for column, column_name in zip(pa_table.columns, pa_table.column_names):
        if not is_deephaven_compatible(column.type):
            print(f"drop column: {column_name} because of unsupported data type {column.type}")
            pa_table = pa_table.drop([column_name])

    # upload the pyarrow table to the Deephaven server
    return dh_session.import_table(pa_table)

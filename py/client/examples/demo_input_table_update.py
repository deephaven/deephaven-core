#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

"""Demo how to send new rows to an input table.

This example requires the Python "grpcio-status" package to be installed for error handling.

The session on localhost:10000 should have a table named "tsv" formatted according to java-client/flight-examples/src/main/java/io/deephaven/client/examples/AddToInputTable.java; additionally.  This can be accomplished with the following snippet:

.. code-block:: python

    from deephaven import input_table, new_table
    from deephaven.column import byte_col, bool_col, char_col, short_col, int_col, long_col, float_col, double_col, string_col, datetime_col

    timestamp = new_table([
        bool_col("Boolean", []),
        byte_col("Byte", []),
        char_col("Char", []),
        short_col("Short", []),
        int_col("Int", []),
        long_col("Long", []),
        float_col("Float", []),
        double_col("Double", []),
        string_col("String", []),
        datetime_col("Instant", []),
    ]).update("ByteVector=(byte[])null")

    it=input_table(init_table=timestamp)
    tsv=it

To produce a structured error, you can validate the range of the Int column with:

.. code-block:: python
    import jpy
    tsv=jpy.get_type("io.deephaven.server.table.inputtables.RangeValidatingInputTable").make(it.j_table, "Int", 0, 32)

"""

from pydeephaven import Session, Table, DHError
from pydeephaven.table import InputTable
import pyarrow as pa
from datetime import datetime
import time
from grpc_status import rpc_status
import grpc
from google.protobuf.any_pb2 import Any
from deephaven_core.proto.inputtable_pb2 import InputTableValidationErrorList

err = None
stat = None
err_list = None


def main():
    global err, stat, err_list
    with Session(host="localhost", port=10000) as dh_session:
        tsv: Table = dh_session.open_table("tsv")
        tsv_input: InputTable = InputTable(dh_session, tsv.ticket)

        for row in range(29, 35):
            column_names = [
                "Boolean",
                "Byte",
                "Char",
                "Short",
                "Int",
                "Long",
                "Float",
                "Double",
                "String",
                "Instant",
            ]

            # Create individual arrays for each value
            # Boolean
            bool_arr = pa.array([True], type=pa.bool_())
            byte_arr = pa.array([100], type=pa.int8())
            char_arr = pa.array([ord("a")], type=pa.uint16())
            short_arr = pa.array([32000], type=pa.int16())
            int_arr = pa.array([row], type=pa.int32())
            long_arr = pa.array([1234567890123], type=pa.int64())
            float_arr = pa.array([3.14], type=pa.float32())
            double_arr = pa.array([3.14], type=pa.float64())
            string_arr = pa.array(["Constrict the World"], type=pa.string())
            timestamp_arr = pa.array([datetime.now()], type=pa.timestamp("us"))

            # Combine all arrays into a table
            pa_table = pa.Table.from_arrays(
                [
                    bool_arr,
                    byte_arr,
                    char_arr,
                    short_arr,
                    int_arr,
                    long_arr,
                    float_arr,
                    double_arr,
                    string_arr,
                    timestamp_arr,
                ],
                names=column_names,
            )

            table = dh_session.import_table(pa_table).update("ByteVector=(byte[])null")
            try:
                tsv_input.add(table)
            except DHError as dhe:
                err = dhe
                parent = dhe.__cause__
                while parent is not None:
                    if isinstance(parent, grpc.Call):
                        stat = rpc_status.from_call(parent)
                        parent = None

                        for detail in stat.details:
                            if (
                                detail.type_url
                                == "type.googleapis.com/"
                                + InputTableValidationErrorList.DESCRIPTOR.full_name
                            ):
                                el = InputTableValidationErrorList()
                                if detail.Unpack(el):
                                    err_list = el
                                    print("Error list", err_list)
                                else:
                                    print("Could not unpack: " + detail)
                    else:
                        parent = parent.__cause__

                raise
            time.sleep(1)


if __name__ == "__main__":
    main()

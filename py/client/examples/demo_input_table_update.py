#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

"""Demo how to chain table operations."""

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
        tsv : Table = dh_session.open_table("tsv")
        tsv_input : InputTable = InputTable(dh_session, tsv.ticket)

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
            char_arr = pa.array([ord('a')], type=pa.uint16())
            short_arr = pa.array([32000], type=pa.int16())
            int_arr = pa.array([row], type=pa.int32())
            long_arr = pa.array([1234567890123], type=pa.int64())
            float_arr = pa.array([3.14], type=pa.float32())
            double_arr = pa.array([3.14], type=pa.float64())
            string_arr = pa.array(["Constrict the World"], type=pa.string())
            timestamp_arr = pa.array([datetime.now()], type=pa.timestamp('us'))

            # Combine all arrays into a table
            pa_table = pa.Table.from_arrays(
                [bool_arr, byte_arr, char_arr, short_arr, int_arr, long_arr,
                 float_arr, double_arr, string_arr, timestamp_arr],
                names=column_names
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
                            if detail.type_url == "type.googleapis.com/" + InputTableValidationErrorList.DESCRIPTOR.full_name:
                                el = InputTableValidationErrorList()
                                if detail.Unpack(el):
                                    err_list = el
                                else:
                                    print("Could not unpack: " + detail)
                    else:
                        parent = parent.__cause__

                raise
            time.sleep(1)

if __name__ == '__main__':
    main()

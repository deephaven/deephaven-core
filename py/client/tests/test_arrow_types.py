#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import unittest
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pydeephaven._arrow import decode_for_pyarrow, map_arrow_type
from pydeephaven.dherror import DHError
from pydeephaven.utils import is_deephaven_compatible
from tests.testbase import BaseTestCase


class ArrowTypesImportTestCase(BaseTestCase):
    def test_import_table_time64(self):
        pa_array = pa.array([1, 2], type=pa.time64("ns"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_import_table_ints(self):
        types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([-1, 0, 127], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_unsigned_ints(self):
        types = [pa.uint16()]
        exception_list = []
        for t in types:
            pa_array = pa.array([0, 255, 65535], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            # print(pa_table, pa_table2)
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_floats(self):
        types = [pa.float32(), pa.float64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([1.111, 2.222], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_strings(self):
        types = [pa.string(), pa.utf8()]
        exception_list = []
        for t in types:
            pa_array = pa.array(["text1", "text2"], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_date32(self):
        types = [pa.date32()]
        exception_list = []
        for t in types:
            pa_array = pa.array([1245, 123456], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_date64(self):
        from datetime import datetime

        pa_date1 = pa.scalar(datetime(2012, 1, 1), type=pa.date64())
        pa_date2 = pa.scalar(datetime(2022, 11, 11), type=pa.date64())
        pa_array = pa.array([pa_date1, pa_date2], type=pa.date64())

        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_null(self):
        pa_data = pa.array([None, None], type=pa.null())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint8(self):
        pa_data = pa.array([2**8 - 1, 0], type=pa.uint8())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint32(self):
        pa_data = pa.array([2**32 - 1, 0], type=pa.uint32())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint64(self):
        pa_data = pa.array([2**64 - 1, 0], type=pa.uint64())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_float16(self):
        # pyarrow requires numpy float16 instances for float16 arrays
        pa_data = pa.array(np.array([1.5, -2.5], dtype=np.float16()), type=pa.float16())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time32_s(self):
        pa_data = pa.array([1000, 2000], type=pa.time32("s"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time32_ms(self):
        pa_data = pa.array([1000, 2000], type=pa.time32("ms"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time64_us(self):
        pa_data = pa.array([1000000, 2000000], type=pa.time64("us"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_s(self):
        pa_data = pa.array([30, 60], type=pa.duration("s"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_ms(self):
        pa_data = pa.array([1000, 2000], type=pa.duration("ms"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_us(self):
        pa_data = pa.array([100000, 200000], type=pa.duration("us"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_ns(self):
        pa_data = pa.array([1000000000, 2000000000], type=pa.duration("ns"))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_month_day_nano(self):
        pa_data = pa.array(
            [(1, 15, 100), (2, 20, 200)], type=pa.month_day_nano_interval()
        )
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_binary(self):
        pa_data = pa.array([b"\x00\x01", b"\x02\x03"], type=pa.binary())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_decimal128(self):
        pa_data = pa.array(
            [Decimal("123.45"), Decimal("-67.89")], type=pa.decimal128(10, 2)
        )
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_decimal256(self):
        pa_data = pa.array(
            [Decimal("123456.78901"), Decimal("-98765.43210")],
            type=pa.decimal256(20, 5),
        )
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_fixed_size_binary(self):
        pa_array = pa.array([b"\x00\x01", b"\x02\x03"], type=pa.binary(2))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_list_int(self):
        list_type = pa.list_(pa.int32())
        pa_array = pa.array([[1, 2], [3, 4]], type=list_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_list_view_int(self):
        lv_type = pa.list_view(pa.int32())
        pa_array = pa.array([[5, 6], [7]], type=lv_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_fixed_sized_list_int(self):
        fsl_type = pa.list_(pa.int32(), 3)
        pa_array = pa.array([[1, 2, 3], [4, 5, 6]], type=fsl_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_map_string_int(self):
        map_type = pa.map_(pa.string(), pa.int32())
        pa_array = pa.array([{"a": 1, "b": 2}, {"c": 3}], type=map_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_dense_union_string_int(self):
        child_str = pa.array(["x", "y"])
        child_int = pa.array([10, 20])
        tags = pa.array([0, 1], type=pa.int8())
        offsets = pa.array([0, 0], type=pa.int32())
        ua = pa.UnionArray.from_dense(
            tags,
            offsets,
            [child_str, child_int],
            field_names=["str", "int"],
            type_codes=[0, 1],
        )
        pa_record_batch = pa.RecordBatch.from_arrays([ua], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_sparse_union_string_int(self):
        child_str = pa.array(["m", "n"])
        child_int = pa.array([7, 8])
        tags = pa.array([1, 0], type=pa.int8())
        ua = pa.UnionArray.from_sparse(
            tags, [child_str, child_int], field_names=["str", "int"], type_codes=[0, 1]
        )
        pa_record_batch = pa.RecordBatch.from_arrays([ua], names=["f1"])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)


class ArrowEncodingsTestCase(BaseTestCase):
    def test_to_arrow_run_end_dictionary(self) -> None:
        # to_arrow decodes run-end over dictionary into dictionary, so to_pandas works.
        self.session.run_script(
            'from deephaven import empty_table\nruns = empty_table(26300).update("S = (ii < 13150) ? `A` : `B`")'
        )

        pa_table = self.session.open_table("runs").to_arrow()

        self.assertTrue(pa.types.is_dictionary(pa_table.schema.field("S").type))
        df = pa_table.to_pandas()
        self.assertEqual({"A": 13150, "B": 13150}, df["S"].value_counts().to_dict())

    def test_input_table_encodings(self) -> None:
        # An input table accepts an encoded schema, and both encoded and plain rows.
        dict_type = pa.dictionary(pa.int32(), pa.string())
        schema = pa.schema(
            [
                pa.field("Dict", dict_type),
                pa.field("Ree", pa.run_end_encoded(pa.int32(), pa.int64()), False),
                pa.field("ReeDict", pa.run_end_encoded(pa.int32(), dict_type), False),
            ]
        )
        dict_array = pa.array(["a", "b", "a"]).dictionary_encode()
        runs = pc.run_end_encode(dict_array.indices)
        encoded = pa.table(
            {
                "Dict": dict_array,
                "Ree": pc.run_end_encode(pa.array([1, 1, 2])),
                "ReeDict": pa.RunEndEncodedArray.from_arrays(
                    runs.run_ends,
                    pa.DictionaryArray.from_arrays(runs.values, dict_array.dictionary),
                ),
            }
        )
        plain = pa.table({"Dict": ["c"], "Ree": [5], "ReeDict": ["d"]})

        input_table = self.session.input_table(schema=schema)
        input_table.add(self.session.import_table(encoded))
        input_table.add(self.session.import_table(plain))

        self.assertEqual(
            {
                "Dict": ["a", "b", "a", "c"],
                "Ree": [1, 1, 2, 5],
                "ReeDict": ["a", "b", "a", "d"],
            },
            input_table.to_arrow().to_pydict(),
        )

    def test_import_table_encodings(self) -> None:
        # import_table accepts dictionary, run-end, and run-end over dictionary encoded columns
        dict_array = pa.array(["a", "b", "a", None, "b", "b"]).dictionary_encode()
        ree_dict_runs = pc.run_end_encode(dict_array.indices)
        pa_table = pa.table(
            {
                "Dict": dict_array,
                "Ree": pc.run_end_encode(pa.array([1, 1, 1, None, 2, 2])),
                "ReeDict": pa.RunEndEncodedArray.from_arrays(
                    ree_dict_runs.run_ends,
                    pa.DictionaryArray.from_arrays(
                        ree_dict_runs.values, dict_array.dictionary
                    ),
                ),
            }
        )

        new_table = self.session.import_table(pa_table)

        self.assertEqual(
            {
                "Dict": ["a", "b", "a", None, "b", "b"],
                "Ree": [1, 1, 1, None, 2, 2],
                "ReeDict": ["a", "b", "a", None, "b", "b"],
            },
            new_table.to_arrow().to_pydict(),
        )
        # The returned table's schema reports the plain column types the server stores
        self.assertEqual(
            [pa.string(), pa.int64(), pa.string()],
            [f.type for f in new_table.schema],
        )

    def test_import_table_round_trip_encodings(self) -> None:
        # A table the server sends with dictionary and run-end encoded columns imports back unchanged
        self.session.run_script(
            "from deephaven import empty_table\n"
            'encoded = empty_table(1000).update(["S = (ii < 500) ? `A` : `B`", "X = ii % 3 == 0 ? `C` : `D`", '
            '"I = (int) (ii / 100)"])'
        )
        pa_table = self.session.open_table("encoded").to_arrow()

        new_table = self.session.import_table(pa_table)

        self.assertEqual(pa_table.to_pydict(), new_table.to_arrow().to_pydict())


class ArrowDecodeTestCase(unittest.TestCase):
    def test_decode_run_end_ordered_dictionary(self) -> None:
        # Decoding run-end over an ordered dictionary keeps the dictionary ordered.
        dict_type = pa.dictionary(pa.int32(), pa.string(), ordered=True)
        runs = pc.run_end_encode(pa.array([0, 0, 1], pa.int32()))
        ree = pa.RunEndEncodedArray.from_arrays(
            runs.run_ends,
            pa.DictionaryArray.from_arrays(
                runs.values, pa.array(["a", "b"]), ordered=True
            ),
        )

        decoded = decode_for_pyarrow(pa.table({"C": ree}))

        self.assertEqual(dict_type, decoded.schema.field("C").type)
        self.assertEqual(["a", "a", "b"], decoded.column("C").to_pylist())

    def test_decode_dictionary_timestamps(self) -> None:
        # Dictionary timestamps, plain or run-end encoded, keep their time zone in to_pandas.
        timestamps = pa.array(
            [0, 0, 10**9, None, 10**9], type=pa.timestamp("ns", tz="UTC")
        )
        dict_array = timestamps.dictionary_encode()
        runs = pc.run_end_encode(dict_array.indices)
        pa_table = pa.table(
            {
                "Dict": dict_array,
                "ReeDict": pa.RunEndEncodedArray.from_arrays(
                    runs.run_ends,
                    pa.DictionaryArray.from_arrays(runs.values, dict_array.dictionary),
                ),
            }
        )

        decoded = decode_for_pyarrow(pa_table)

        expected = pa.table({"T": timestamps}).to_pandas()["T"]
        for name in ["Dict", "ReeDict"]:
            self.assertEqual(timestamps.type, decoded.schema.field(name).type, name)
            self.assertTrue(
                expected.equals(decoded.to_pandas()[name].rename("T")), name
            )

    def test_map_arrow_type_error_names_encoded_type(self) -> None:
        # The error names the encoded type from the user's schema.
        with self.assertRaises(DHError) as cm:
            map_arrow_type(pa.dictionary(pa.int8(), pa.large_string()))

        self.assertIn("dictionary<values=large_string", str(cm.exception))
        self.assertIn("pydeephaven.utils.is_deephaven_compatible", str(cm.exception))

    def test_is_deephaven_compatible(self) -> None:
        # Exact, parameterized, and encoded types pass when their values and elements are supported.
        string_dict = pa.dictionary(pa.int32(), pa.string())
        supported = [
            pa.int64(),
            pa.decimal128(5, 2),
            string_dict,
            pa.run_end_encoded(pa.int32(), string_dict),
            pa.list_(pa.int64()),
            pa.list_(string_dict),
            pa.map_(pa.string(), pa.decimal128(5, 2)),
        ]
        unsupported = [
            pa.large_string(),
            pa.dictionary(pa.int32(), pa.large_string()),
            pa.struct([pa.field("a", pa.int64())]),
            pa.list_(pa.large_string()),
            pa.list_(pa.struct([pa.field("a", pa.int64())])),
            pa.map_(pa.string(), pa.large_string()),
            pa.dense_union(
                [pa.field("s", pa.string()), pa.field("l", pa.large_string())]
            ),
        ]

        for data_type in supported:
            self.assertTrue(is_deephaven_compatible(data_type), data_type)
        for data_type in unsupported:
            self.assertFalse(is_deephaven_compatible(data_type), data_type)


if __name__ == "__main__":
    unittest.main()

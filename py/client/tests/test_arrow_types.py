#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import unittest
from time import sleep, time

import numpy as np
import pyarrow as pa
import pandas as pd
from pyarrow import csv
from decimal import Decimal

from tests.testbase import BaseTestCase


class ArrowTypesImportTestCase(BaseTestCase):

    def test_import_table_time64(self):
        pa_array = pa.array([1, 2], type=pa.time64('ns'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_import_table_ints(self):
        types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([-1, 0, 127], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
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
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
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
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
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
            pa_array = pa.array(['text1', "text2"], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
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
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
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

        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_null(self):
        pa_data = pa.array([None, None], type=pa.null())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint8(self):
        pa_data = pa.array([2**8 - 1, 0], type=pa.uint8())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint32(self):
        pa_data = pa.array([2**32 - 1, 0], type=pa.uint32())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_uint64(self):
        pa_data = pa.array([2**64 - 1, 0], type=pa.uint64())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_float16(self):
        # pyarrow requires numpy float16 instances for float16 arrays
        pa_data = pa.array(np.array([1.5, -2.5], dtype=np.float16()), type=pa.float16())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time32_s(self):
        pa_data = pa.array([1000, 2000], type=pa.time32('s'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time32_ms(self):
        pa_data = pa.array([1000, 2000], type=pa.time32('ms'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_time64_us(self):
        pa_data = pa.array([1000000, 2000000], type=pa.time64('us'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_s(self):
        pa_data = pa.array([30, 60], type=pa.duration('s'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_ms(self):
        pa_data = pa.array([1000, 2000], type=pa.duration('ms'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_us(self):
        pa_data = pa.array([100000, 200000], type=pa.duration('us'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_duration_ns(self):
        pa_data = pa.array([1000000000, 2000000000], type=pa.duration('ns'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_month_day_nano(self):
        pa_data = pa.array([(1, 15, 100), (2, 20, 200)], type=pa.month_day_nano_interval())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_binary(self):
        pa_data = pa.array([b'\x00\x01', b'\x02\x03'], type=pa.binary())
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_decimal128(self):
        pa_data = pa.array([Decimal('123.45'), Decimal('-67.89')], type=pa.decimal128(10, 2))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_types_decimal256(self):
        pa_data = pa.array([Decimal('123456.78901'), Decimal('-98765.43210')], type=pa.decimal256(20, 5))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_data], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_fixed_size_binary(self):
        pa_array = pa.array([b'\x00\x01', b'\x02\x03'],
                            type=pa.binary(2))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_list_int(self):
        list_type = pa.list_(pa.int32())
        pa_array = pa.array([[1, 2], [3, 4]], type=list_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_list_view_int(self):
        lv_type = pa.list_view(pa.int32())
        pa_array = pa.array([[5, 6], [7]], type=lv_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_fixed_sized_list_int(self):
        fsl_type = pa.list_(pa.int32(), 3)
        pa_array = pa.array([[1, 2, 3], [4, 5, 6]], type=fsl_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_map_string_int(self):
        map_type = pa.map_(pa.string(), pa.int32())
        pa_array = pa.array([{'a': 1, 'b': 2}, {'c': 3}], type=map_type)
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_dense_union_string_int(self):
        child_str = pa.array(['x', 'y'])
        child_int = pa.array([10, 20])
        tags = pa.array([0, 1], type=pa.int8())
        offsets = pa.array([0, 0], type=pa.int32())
        ua = pa.UnionArray.from_dense(tags, offsets,
                                      [child_str, child_int],
                                      field_names=['str', 'int'],
                                      type_codes=[0, 1]
                                      )
        pa_record_batch = pa.RecordBatch.from_arrays([ua], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_arrow_sparse_union_string_int(self):
        child_str = pa.array(['m', 'n'])
        child_int = pa.array([7, 8])
        tags = pa.array([1, 0], type=pa.int8())
        ua = pa.UnionArray.from_sparse(tags,
                                       [child_str, child_int],
                                       field_names=['str', 'int'],
                                       type_codes=[0, 1]
                                       )
        pa_record_batch = pa.RecordBatch.from_arrays([ua], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

if __name__ == '__main__':
    unittest.main()

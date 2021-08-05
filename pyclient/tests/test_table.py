import time

import pyarrow as pa
from pyarrow import csv

from tests.testbase import BaseTestCase


class TableTestCase(BaseTestCase):

    def test_update(self):
        t = self.session.time_table(period=10000000)
        column_specs = ["Col1 = i", "Col2 = i * 2"]
        t2 = t.update(column_specs=column_specs)
        # time table has a default timestamp column
        self.assertEqual(len(column_specs) + 1, len(t2.schema))

    def test_snapshot_timetable(self):
        t = self.session.time_table(period=10000000)
        time.sleep(1)
        pa_table = t.snapshot()
        self.assertGreaterEqual(pa_table.num_rows, 1)

    def test_import_table_long_csv(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.snapshot()
        self.assertEqual(pa_table2, pa_table)
        df = pa_table2.to_pandas()
        self.assertEquals(1000, len(df.index))

    def test_import_table_time64(self):
        pa_array = pa.array([1, 2], type=pa.time64('ns'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.snapshot()
        self.assertEqual(pa_table, pa_table2)

    def test_import_table_ints(self):
        types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([-1, 0, 127], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.snapshot()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_unsigned_ints(self):
        types = [pa.uint16()]
        exception_list = []
        for t in types:
            pa_array = pa.array([0, 255, 65535], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_dates(self):
        types = [pa.date32(), pa.date64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([1245, 123456], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.snapshot()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_create_data_table_then_update(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table).update(column_specs=['Sum=a+b+c+d'])
        pa_table2 = new_table.snapshot()
        df = pa_table2.to_pandas()
        self.assertEquals(df.shape[1], 6)
        self.assertEquals(1000, len(df.index))

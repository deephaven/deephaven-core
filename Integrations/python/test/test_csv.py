#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven import Types
from deephaven import read_csv, write_csv
from unittest import TestCase

def get_col_names(t):
    return  [col for col in t.getDefinition().getColumnNamesArray()]


class CsvTestCase(TestCase):
    def test_read_simple(self):
        t = read_csv("test/data/small_sample.csv")
        col_names = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge".split(",")
        t_col_names = get_col_names(t)
        self.assertEqual(col_names, t_col_names)

    def test_read_header(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [Types.string, Types.int64, Types.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('test/data/test_csv.csv', header=table_header)
        t_col_names = get_col_names(t)
        self.assertEqual(col_names, t_col_names)

    def test_read_error_col_type(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [Types.string, Types.float_, Types.int64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(Exception) as cm:
            t = read_csv('test/data/test_csv.csv', header=table_header)

    def test_read_error_quote(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [Types.string, Types.int64, Types.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(Exception) as cm:
            t = read_csv('test/data/test_csv.csv', header=table_header, quote=",")

    def test_write(self):
        t = read_csv("test/data/small_sample.csv")
        write_csv(t, "./test_write.csv")
        t_cols = get_col_names(t)
        t = read_csv("./test_write.csv")
        self.assertEqual(t_cols, get_col_names(t))

        col_names = ["Strings", "Longs", "Floats"]
        col_types = [Types.string, Types.long_, Types.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('test/data/test_csv.csv', header=table_header)
        write_csv(t, "./test_write.csv", cols=col_names)
        t = read_csv('./test_write.csv')
        self.assertEqual(col_names, get_col_names(t))

        import os
        os.remove("./test_write.csv")


if __name__ == '__main__':
    unittest.main()

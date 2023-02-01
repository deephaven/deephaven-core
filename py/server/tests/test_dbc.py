#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

import turbodbc

from deephaven.dbc import odbc as dhodbc, adbc as dhadbc
from tests.testbase import BaseTestCase


# noinspection SqlDialectInspection
class DbcTestCase(BaseTestCase):

    def test_read_odbc(self):
        connection_string = 'Driver={PostgreSQL};Server=postgres;Port=5432;Database=test;Uid=test;Pwd=test;'
        with turbodbc.connect(connection_string=connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT t_ts, t_id, t_instrument, t_exchange, t_price, t_size FROM CRYPTO_TRADES")
                table = dhodbc.read_cursor(cursor)
                self.assertEqual(table.size, cursor.rowcount)

    def test_read_adbc(self):
        import adbc_driver_postgresql.dbapi

        uri = "postgresql://postgres:5432/test?user=test&password=test"
        with adbc_driver_postgresql.dbapi.connect(uri) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT t_ts, t_exchange, t_price, t_size FROM CRYPTO_TRADES LIMIT 10")
                table = dhadbc.read_cursor(cursor)
                # This is not ideal but ADBC might have a bug regarding cursor.rowcount it currently returns -1
                # instead of the actual size
                self.assertEqual(table.size, 10)


if __name__ == '__main__':
    unittest.main()

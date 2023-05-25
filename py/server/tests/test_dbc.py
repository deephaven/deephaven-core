#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import platform
import unittest

import turbodbc

from deephaven import DHError, read_sql
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

    @unittest.skipIf(platform.machine() != "x86_64", reason="connectorx not available on Linux/Arm64"
                                                            "https://github.com/sfu-db/connector-x/issues/386")
    def test_read_sql_connectorx(self):
        query = "SELECT t_ts, t_id, t_instrument, t_exchange, t_price, t_size FROM CRYPTO_TRADES LIMIT 10"
        postgres_url = "postgresql://test:test@postgres:5432/test"
        dh_table = read_sql(conn=postgres_url, query=query)
        self.assertEqual(len(dh_table.columns), 6)
        self.assertEqual(dh_table.size, 10)

        with self.assertRaises(DHError) as cm:
            dh_table = read_sql(conn="garbage", query=query)

    def test_read_sql(self):
        query = "SELECT t_ts, t_id, t_instrument, t_exchange, t_price, t_size FROM CRYPTO_TRADES LIMIT 10"

        with self.subTest("odbc"):
            connection_string = 'Driver={PostgreSQL};Server=postgres;Port=5432;Database=test;Uid=test;Pwd=test;'
            dh_table = read_sql(conn=connection_string, query=query, driver="odbc")
            self.assertEqual(len(dh_table.columns), 6)
            self.assertEqual(dh_table.size, 10)

        with self.subTest("adbc"):
            uri = "postgresql://postgres:5432/test?user=test&password=test"
            dh_table = read_sql(conn=uri, query=query, driver="adbc")
            self.assertEqual(len(dh_table.columns), 6)
            self.assertEqual(dh_table.size, 10)

        with self.subTest("odbc-connection"):
            connection_string = 'Driver={PostgreSQL};Server=postgres;Port=5432;Database=test;Uid=test;Pwd=test;'
            with turbodbc.connect(connection_string=connection_string) as conn:
                dh_table = read_sql(conn=conn, query=query, driver="odbc")
                self.assertEqual(len(dh_table.columns), 6)
                self.assertEqual(dh_table.size, 10)

        with self.subTest("adbc-connection"):
            import adbc_driver_postgresql.dbapi
            uri = "postgresql://postgres:5432/test?user=test&password=test"
            with adbc_driver_postgresql.dbapi.connect(uri) as conn:
                dh_table = read_sql(conn=conn, query=query, driver="adbc")
                self.assertEqual(len(dh_table.columns), 6)
                self.assertEqual(dh_table.size, 10)

        with self.assertRaises(DHError) as cm:
            dh_table = read_sql(conn=[None], query=query, driver="adbc")

        with self.assertRaises(DHError) as cm:
            dh_table = read_sql(conn="garbage", query=query, driver="adbc")

        with self.assertRaises(Exception) as cm:
            dh_table = read_sql(conn="garbage", query=query, driver="odbc")


if __name__ == '__main__':
    unittest.main()

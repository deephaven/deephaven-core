#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import os
import unittest

import pyarrow.flight as flight
from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect
from flightsql import FlightSQLClient

psk = "deephaven"
host = os.environ.get("DH_HOST", "localhost")


class CookieMiddleware(flight.ClientMiddleware):
    """Middleware to handle cookie-based authentication."""

    def __init__(self):
        self.cookie_store = None  # Store the session cookie

    def sending_headers(self):
        """Attach cookie to outgoing requests."""
        headers = {}
        if self.cookie_store:
            headers["cookie"] = self.cookie_store
        return headers

    def received_headers(self, headers):
        """Capture and store the Set-Cookie header from the server response."""
        if "set-cookie" in headers:
            self.cookie_store = headers["set-cookie"]


class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """Factory to create middleware instances per request."""

    def __init__(self):
        self.middleware = CookieMiddleware()

    def start_call(self, info):
        """Returns the same middleware instance for all calls to maintain cookies."""
        return self.middleware


# Create the client with the middleware
middleware_factory = CookieMiddlewareFactory()


class FlightSQLTestCase(unittest.TestCase):
    def test_flight_sql_client(self):
        client = FlightSQLClient(
            host=host,
            port=10000,
            insecure=True,
            token=f"io.deephaven.authentication.psk.PskAuthenticationHandler {psk}",
            metadata={"x-deephaven-auth-cookie-request": "true"},
            middleware=[middleware_factory],
        )
        expected_cell_value = 88
        flight_info = client.execute(f"SELECT {expected_cell_value}")

        # Extract the token for retrieving data
        ticket = flight_info.endpoints[0].ticket

        # Use the ticket to request the Arrow data stream.
        # Return a FlightStreamReader for streaming the results.
        reader = client.do_get(ticket)

        # Read all data to a pyarrow.Table
        table = reader.read_all()
        self.assertEqual(table[0][0].as_py(), expected_cell_value)
        client.close()

    def test_flight_sql_adbc(self):
        expected_cell_value = 88
        with connect(
            # "grpc+tls://localhost:10000",
            f"grpc://{host}:10000",
            db_kwargs={
                DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven"
            },
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT {expected_cell_value}")
                data = cursor.fetchall()
                self.assertEqual(data[0][0], expected_cell_value)

                cursor.execute(f"SELECT {expected_cell_value + 10}")
                data = cursor.fetchall()
                self.assertEqual(data[0][0], expected_cell_value + 10)


if __name__ == "__main__":
    unittest.main()

---
title: Connecting to a Deephaven Flight SQL server
sidebar_label: Flight SQL
---

[Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) is a protocol on top of
[Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) that exposes SQL-like capabilities.

The Deephaven Flight SQL server exposes Deephaven tables from the global query scope as named tables that can be accessed and queried via the default Flight SQL catalog. It's a built-in part of the Deephaven server and listens on the same port.

> [!NOTE]
> `SELECT` queries are supported; `INSERT`, `UPDATE`, and `DELETE` queries are not currently supported.

While Flight SQL may be an easy jumping-off point, the Flight SQL APIs are not [Live Dataframe APIs](../conceptual/table-update-model.md).
If you need to receive real-time updates, the Deephaven-native clients are the best option.

## Examples using anonymous authentication

Each of the following examples demonstrates:

1. Connecting to an insecure (w/o TLS) Deephaven Flight SQL server on localhost with port 10000.
2. Using [Anonymous authentication](../how-to-guides/authentication/auth-anon.md).
3. Executing the SQL command `SELECT 42 as Foo`. Note, to run a more meaningful query, such as "SELECT \* from StockQuotes", you need to have a table "StockQuotes" in the global query scope first.
4. Printing out the results.

> [!NOTE]
> Additional configuration options for TLS and authentication will likely be needed when connecting Flight SQL clients to production servers.
>
> If the Flight SQL client expects to use cookies for authentication, the `x-deephaven-auth-cookie-request` header must be set to `true`.

### ADBC Flight SQL

[Arrow Database Connectivity (ADBC)](https://arrow.apache.org/docs/format/ADBC.html) is an Arrow-first interface for efficiently fetching large datasets from a database.
It's a great choice when paired with the Deephaven Flight SQL server given that the Flight protocol itself is Arrow-first.

There are a variety of ADBC client implementations that have [Flight SQL drivers](https://arrow.apache.org/adbc/current/driver/flight_sql.html).

#### ADBC Python

The [ADBC Python](https://arrow.apache.org/adbc/current/python/index.html) library has a [Flight SQL driver](https://arrow.apache.org/adbc/current/python/api/adbc_driver_flightsql.html), and is simple to use from Python.
[Installation](https://arrow.apache.org/adbc/main/driver/installation.html#python) is as simple as `pip install adbc-driver-flightsql pyarrow`.

```python skip-test
import adbc_driver_flightsql.dbapi
from adbc_driver_flightsql import DatabaseOptions

with adbc_driver_flightsql.dbapi.connect(
    "grpc://localhost:10000",
    db_kwargs={
        DatabaseOptions.AUTHORIZATION_HEADER.value: "Anonymous",
    },
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT 42 as Foo")
        pa_table = cursor.fetch_arrow_table()
        print(pa_table)
```

#### ADBC Java

The [ADBC Java](https://arrow.apache.org/adbc/current/java/index.html) library has a [Flight SQL driver](https://arrow.apache.org/adbc/current/java/api/org/apache/arrow/adbc/driver/flightsql/package-summary.html), and is simple to use from Java.
[Installation](https://arrow.apache.org/adbc/main/driver/installation.html#java) requires a dependency on `org.apache.arrow.adbc:adbc-driver-flight-sql`.

```java skip-test
Map<String, Object> options = new HashMap<>();
AdbcDriver.PARAM_URI.set(options, "grpc://localhost:10000");
FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.set(options, true);
options.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "Authorization", "Anonymous");
options.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "x-deephaven-auth-cookie-request", "true");
try (
        BufferAllocator allocator = new RootAllocator();
        AdbcDatabase database = new FlightSqlDriverFactory().getDriver(allocator).open(options);
        AdbcConnection connection = database.connect()) {
    try (AdbcStatement statement = connection.createStatement()) {
        statement.setSqlQuery("SELECT 42 as Foo");
        try (QueryResult queryResult = statement.executeQuery()) {
            ArrowReader reader = queryResult.getReader();
            VectorSchemaRoot vectorRoot = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                System.out.println(vectorRoot.contentToTSVString());
            }
        }
    }
}
```

#### Other ADBC clients

Any ADBC client that has a Flight SQL driver should work with Deephaven Flight SQL:

- [ADBC C / C++](https://arrow.apache.org/adbc/current/cpp/index.html) [driver and installation](https://arrow.apache.org/adbc/main/driver/installation.html#c-c).
- [ADBC Go](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc) [driver](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/driver/flightsql) and [installation](https://arrow.apache.org/adbc/current/driver/installation.html#go).
- [ADBC R](https://arrow.apache.org/adbc/current/r/index.html) [driver](https://arrow.apache.org/adbc/current/r/adbcflightsql/index.html) and [installation](https://arrow.apache.org/adbc/main/driver/installation.html#r).

### Flight SQL JDBC Driver

The [Flight SQL JDBC Driver](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html) can be used directly from Java,
or from other applications that support [Java Database Connectivity (JDBC)](https://docs.oracle.com/javase/tutorial/jdbc/overview/index.html).
When already working in the context of JDBC, the Flight SQL JDBC Driver can be used to access the Deephaven Flight SQL server.
When the client is not required to use JDBC, it is advisable to use ADBC or the Flight SQL client directly.

```java skip-test
String url = "jdbc:arrow-flight-sql://localhost:10000?useEncryption=0&Authorization=Anonymous&x-deephaven-auth-cookie-request=true";
try (Connection connection = DriverManager.getConnection(url)) {
    try (
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 42 As Foo")) {
        while (resultSet.next()) {
            System.out.println(resultSet.getLong("Foo"));
        }
    }
}
```

### Flight SQL Client (Java)

The [Flight SQL Client](https://arrow.apache.org/java/current/index.html) can be used directly from Java.
This is a good choice when the user needs explicit control over Flight SQL RPCs or needs direct access to Flight APIs.

```java skip-test
FlightClient client;
{
    client = FlightClient.builder()
        .allocator(new RootAllocator())
        .location(Location.forGrpcInsecure("localhost", 10000))
        .intercept(new ClientCookieMiddleware.Factory())
        .build();
    {
        CallHeaders headers = new FlightCallHeaders();
        headers.insert("Authorization", "Anonymous");
        headers.insert("x-deephaven-auth-cookie-request", "true");
        client.handshake(new HeaderCallOption(headers));
    }
}
try (FlightSqlClient flightSqlClient = new FlightSqlClient(client)) {
    FlightInfo info = flightSqlClient.execute("SELECT 42 as Foo");
    try (FlightStream stream = flightSqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
        while (stream.next()) {
            System.out.println(stream.getRoot().contentToTSVString());
        }
    }
}
```

## Examples using PSK (pre-shared key) authentication

Each of the following examples demonstrates:

1. Connecting to an insecure (w/o TLS) Deephaven Flight SQL server on localhost with port 10000.
2. Authenticating with a [Pre-shared Key (PSK)](../how-to-guides/authentication/auth-psk.md). The PSK used in the examples is `deephaven`.
3. Executing the SQL command `SELECT 42 as Foo`. Note, to run a more meaningful query, such as "SELECT \* from StockQuotes", you need to have a table "StockQuotes" in the global query scope first.
4. Fetching the query results.
5. Printing out the results.

#### ADBC Python

The [ADBC Python](https://arrow.apache.org/adbc/current/python/index.html) library has a [Flight SQL driver](https://arrow.apache.org/adbc/current/python/api/adbc_driver_flightsql.html), and is simple to use from Python.
[Installation](https://arrow.apache.org/adbc/main/driver/installation.html#python) is as simple as `pip install adbc-driver-flightsql pyarrow`.

```python skip-test
from typing import List, Tuple
from adbc_driver_flightsql import dbapi, DatabaseOptions

with dbapi.connect(
    "grpc://localhost:10000",
    db_kwargs={
        DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven",
    },
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT 42 as Foo")
        rows: List[Tuple] = cursor.fetchall()
        print(rows[0][0])
```

#### Python Flight SQL Client

The [Python Flight SQL client](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/flight/python-flightsql-dbapi/) offers both a [DB API 2](https://peps.python.org/pep-0249/) interface and an Arrow Flight interface for interacting with Flight SQL. To get started, install the client by running the following command:

```bash skip-test
pip install flightsql-dbapi
```

This will set up the necessary environment to try out the example.

```python skip-test
import pyarrow.flight as flight
from flightsql import FlightSQLClient


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


middleware_factory = CookieMiddlewareFactory()

# Create the client with the middleware
client = FlightSQLClient(
    host=host,
    port=10000,
    insecure=True,
    token=f"io.deephaven.authentication.psk.PskAuthenticationHandler deephaven",
    metadata={"x-deephaven-auth-cookie-request": "true"},
    middleware=[middleware_factory],
)

flight_info = client.execute("SELECT 42 as Foo")

ticket = flight_info.endpoints[0].ticket

reader = client.do_get(ticket)

pa_table = reader.read_all()
print(pa_table[0][0])
client.close()
```

## Tips

Try experimenting with other queries once you have these simple examples running.
For example, create some named Deephaven tables on the server via the Web UI:

```python skip-test
from deephaven import time_table

my_table_1 = (
    time_table("PT1s").view(["Timestamp1=Timestamp", "Id=ii % 11"]).last_by(["Id"])
)
my_table_2 = (
    time_table("PT5s").view(["Timestamp2=Timestamp", "Id=ii % 11"]).last_by(["Id"])
)
```

And execute the Flight SQL commands:

```sql
SELECT * FROM my_table_1
```

```sql
SELECT
  *
FROM
  my_table_2
ORDER BY
  Timestamp2
```

```sql
SELECT
  my_table_1.Id,
  my_table_1.Timestamp1,
  my_table_2.Timestamp2
FROM
  my_table_1
  INNER JOIN my_table_2 ON my_table_1.Id = my_table_2.Id
```

## What's next

[Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/reference/odbc-overview) support via Flight SQL is coming soon.

## Related documentation

- [Introducing Arrow Flight SQL](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Deephaven Flight SQL examples repository](https://github.com/devinrsmith/deephaven-flightsql-examples)

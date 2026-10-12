---
title: Connecting to a Deephaven Flight SQL server
---

[Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) is a protocol on top of
[Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) that exposes SQL-like capabilities.

The Deephaven Flight SQL server exposes each table in the server's [global query scope](./query-scope.md) as a named table. A Flight SQL query refers to a table by its variable name, as in `SELECT * FROM my_table`. The Flight SQL server is a built-in part of the Deephaven server and listens on the same port as the rest of the server, which is 10000 by default.

> [!NOTE]
> The Deephaven Flight SQL server supports `SELECT` queries. It doesn't support `INSERT`, `UPDATE`, or `DELETE` queries.

Flight SQL queries return static snapshots rather than [live, ticking tables](../conceptual/table-update-model.md).
If you need to receive real-time updates, use a [Deephaven client API](../conceptual/client-server-model.md#client-apis) that supports ticking tables, such as the Java, JavaScript, or C++ client, or the Python client with the `pydeephaven-ticking` package.

## Authenticate

The Deephaven server uses [pre-shared key (PSK) authentication](./authentication/auth-psk.md) by default. If you don't set the `authentication.psk` property, the server generates a random PSK at startup and prints it to the server log.

Every example except the [Flight SQL JDBC Driver](#flight-sql-jdbc-driver) example uses the PSK `deephaven`. The JDBC example uses anonymous authentication. To match the PSK examples, do one of the following:

- Start the server with `-Dauthentication.psk=deephaven`.
- Replace `deephaven` in the examples with the key from the server log.

To use [anonymous authentication](./authentication/auth-anon.md) instead, start the server with `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`. This replaces PSK authentication. To run every example on this page against one server, enable both methods: `-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler,io.deephaven.authentication.psk.PskAuthenticationHandler -Dauthentication.psk=deephaven`.

Each example passes the authentication method differently:

- The [ADBC](#adbc) (Arrow Database Connectivity) examples send it in an authorization header, which they set with the `adbc.flight.sql.authorization_header` option.
- The [Flight SQL client (Java)](#flight-sql-client-java) example sends it in an `Authorization` header.
- The [Flight SQL JDBC Driver](#flight-sql-jdbc-driver) example sets it with the `Authorization` URL parameter.
- The [Python Flight SQL client](#python-flight-sql-client) example sets it with the client's `token` argument. That client adds the `Bearer` prefix itself, so pass the rest of the PSK header value without it: `io.deephaven.authentication.psk.PskAuthenticationHandler deephaven`.

The following table lists the authorization header value for each authentication method:

| Authentication method | Authorization header value                                                  |
| --------------------- | --------------------------------------------------------------------------- |
| PSK                   | `Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven` |
| Anonymous             | `Anonymous`                                                                 |

Every example except ADBC Python also sets the `x-deephaven-auth-cookie-request` header to `true`. This header asks the server to return the session token as a cookie. The client then sends that cookie with each later request. Each example handles the cookie differently:

- The ADBC Java example sets the `adbc.flight.sql.rpc.with_cookie_middleware` option.
- The Flight SQL JDBC Driver handles cookies itself, so its example only sets the header with the `x-deephaven-auth-cookie-request=true` URL parameter.
- The Flight SQL client (Java) example uses `ClientCookieMiddleware`.
- The Python Flight SQL client example defines a `CookieMiddleware` class.

Some clients, such as the Flight SQL JDBC Driver, don't keep the session without the cookie request header. If your client fails to reuse its session after authenticating, set this header.

## Connect with a client

Each example in this section connects to an insecure (without TLS) Deephaven Flight SQL server on localhost with port 10000, runs the SQL query `SELECT 42 as Foo`, and prints the results.

> [!NOTE]
> Production servers typically require TLS and additional authentication configuration, so adjust the connection settings in these examples to match your server.

Choose a client:

- [ADBC](#adbc) is the recommended choice for new code in Python, Java, and other languages.
- The [Flight SQL JDBC Driver](#flight-sql-jdbc-driver) is for applications that require JDBC.
- The [Flight SQL client (Java)](#flight-sql-client-java) gives explicit control over Flight SQL RPCs.
- The [Python Flight SQL client](#python-flight-sql-client) is archived. Use it only if existing code already depends on it.

### ADBC

[Arrow Database Connectivity (ADBC)](https://arrow.apache.org/docs/format/ADBC.html) is a database interface that returns query results in the Arrow columnar format, which makes it efficient for fetching large datasets.
It pairs well with the Deephaven Flight SQL server because Flight also transfers data in the Arrow format, so results don't need conversion.

The recommended way to get the [Flight SQL driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html) is to install it with the [dbc](https://docs.columnar.tech/dbc/) command-line tool:

```bash skip-test
dbc install flightsql
```

This installs the driver's shared library and its driver manifest to a location where an ADBC client library can find the driver by the name `flightsql`. Any language with an [ADBC client library](https://arrow.apache.org/adbc/current/client_libraries.html), also called a driver manager, can load the driver.

#### ADBC Python

The [ADBC Python](https://arrow.apache.org/adbc/current/python/index.html) library (driver manager) loads the `flightsql` driver installed with `dbc install flightsql`.
The library implements Python's standard database interface, [DB API 2](https://peps.python.org/pep-0249/). Its DB API module needs either `pyarrow` or `polars`.
To [install](https://arrow.apache.org/adbc/current/python/quickstart.html) the library with `pyarrow`, which this example uses, run:

```bash skip-test
pip install adbc-driver-manager pyarrow
```

```python skip-test
from adbc_driver_manager import dbapi

with dbapi.connect(
    driver="flightsql",
    db_kwargs={
        "uri": "grpc://localhost:10000",
        "adbc.flight.sql.authorization_header": "Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven",
    },
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 42 as Foo")
        pa_table = cursor.fetch_arrow_table()
        print(pa_table)
```

This example doesn't set the `x-deephaven-auth-cookie-request` header. If the connection fails to reuse its session after authenticating, add the same two options that the [ADBC Java](#adbc-java) example uses to the `db_kwargs` dictionary:

- `"adbc.flight.sql.rpc.with_cookie_middleware": "true"`
- `"adbc.flight.sql.rpc.call_header.x-deephaven-auth-cookie-request": "true"`

#### ADBC Java

The [ADBC Java](https://arrow.apache.org/adbc/current/java/index.html) library (driver manager) loads the `flightsql` driver installed with `dbc install flightsql` through the Java Native Interface (JNI).
[Installation](https://arrow.apache.org/adbc/current/java/quickstart.html) requires a dependency on `org.apache.arrow.adbc:adbc-driver-jni`.

```java skip-test
final String DRIVER_FACTORY = "org.apache.arrow.adbc.driver.jni.JniDriverFactory";

Map<String, Object> params = new HashMap<>();
JniDriver.PARAM_DRIVER.set(params, "flightsql");
params.put("uri", "grpc://localhost:10000");
params.put(
        "adbc.flight.sql.authorization_header",
        "Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven");
params.put("adbc.flight.sql.rpc.with_cookie_middleware", "true");
params.put("adbc.flight.sql.rpc.call_header.x-deephaven-auth-cookie-request", "true");
try (
        BufferAllocator allocator = new RootAllocator();
        AdbcDatabase database =
                AdbcDriverManager.getInstance().connect(DRIVER_FACTORY, allocator, params);
        AdbcConnection connection = database.connect()) {
    try (AdbcStatement statement = connection.createStatement()) {
        statement.setSqlQuery("SELECT 42 as Foo");
        try (AdbcStatement.QueryResult queryResult = statement.executeQuery()) {
            ArrowReader reader = queryResult.getReader();
            while (reader.loadNextBatch()) {
                System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
            }
        }
    }
}
```

#### Other ADBC clients

Other ADBC client libraries also work with Deephaven Flight SQL:

- [ADBC C / C++](https://arrow.apache.org/adbc/current/cpp/index.html) (see its [driver manager](https://arrow.apache.org/adbc/current/cpp/driver_manager.html))
- [ADBC C# / .NET](https://arrow.apache.org/adbc/current/csharp/index.html)
- [ADBC Go](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc) (see its [driver manager](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/drivermgr))
- [ADBC JavaScript / TypeScript](https://arrow.apache.org/adbc/current/javascript/index.html) (see its [driver manager](https://arrow.apache.org/adbc/current/javascript/driver_manager.html))
- [ADBC R](https://arrow.apache.org/adbc/current/r/index.html)
- [ADBC Ruby](https://arrow.apache.org/adbc/current/ruby/index.html)
- [ADBC Rust](https://arrow.apache.org/adbc/current/rust/index.html) (see its [driver manager](https://arrow.apache.org/adbc/current/rust/driver_manager.html))

### Flight SQL JDBC Driver

Use the [Flight SQL JDBC Driver](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html) to connect to the Deephaven Flight SQL server from Java or from any application that supports [Java Database Connectivity (JDBC)](https://docs.oracle.com/javase/tutorial/jdbc/overview/index.html).
If your client doesn't need JDBC, use [ADBC](#adbc) or the [Flight SQL client (Java)](#flight-sql-client-java) instead.
To use the driver, add a dependency on `org.apache.arrow:flight-sql-jdbc-driver`.

This example uses anonymous authentication. The `Authorization=Anonymous` URL parameter selects it. To run it, start the server with anonymous authentication enabled, as [Authenticate](#authenticate) describes.

```java skip-test
String url = "jdbc:arrow-flight-sql://localhost:10000?useEncryption=0&Authorization=Anonymous&x-deephaven-auth-cookie-request=true";
try (Connection connection = DriverManager.getConnection(url)) {
    try (
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 42 as Foo")) {
        while (resultSet.next()) {
            System.out.println(resultSet.getLong("Foo"));
        }
    }
}
```

### Flight SQL client (Java)

Use the [Flight SQL client](https://arrow.apache.org/java/current/flight_sql.html) directly from Java when you need explicit control over Flight SQL RPCs or direct access to Flight APIs.
To use the client, add a dependency on `org.apache.arrow:flight-sql`.

```java skip-test
try (
        BufferAllocator allocator = new RootAllocator();
        FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure("localhost", 10000))
                .intercept(new ClientCookieMiddleware.Factory())
                .build()) {
    CallHeaders headers = new FlightCallHeaders();
    headers.insert(
            "Authorization",
            "Bearer io.deephaven.authentication.psk.PskAuthenticationHandler deephaven");
    headers.insert("x-deephaven-auth-cookie-request", "true");
    client.handshake(new HeaderCallOption(headers));

    FlightSqlClient flightSqlClient = new FlightSqlClient(client);
    FlightInfo info = flightSqlClient.execute("SELECT 42 as Foo");
    try (FlightStream stream = flightSqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
        while (stream.next()) {
            System.out.println(stream.getRoot().contentToTSVString());
        }
    }
}
```

### Python Flight SQL client

The [Python Flight SQL client](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/flight/python-flightsql-dbapi/) offers both a [DB API 2](https://peps.python.org/pep-0249/) interface and an Arrow Flight interface for interacting with Flight SQL. The `flightsql-dbapi` project is archived and no longer maintained, so prefer the [ADBC Python](#adbc-python) client for new code. Use this client only if your existing code already depends on it.

This example authenticates with the PSK through the client's `token` argument. To install the client, run:

```bash skip-test
pip install flightsql-dbapi
```

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
    """Factory that returns one shared middleware instance so the session cookie persists across calls."""

    def __init__(self):
        self.middleware = CookieMiddleware()

    def start_call(self, info):
        """Returns the same middleware instance for all calls to maintain cookies."""
        return self.middleware


middleware_factory = CookieMiddlewareFactory()

# Create the client with the middleware
client = FlightSQLClient(
    host="localhost",
    port=10000,
    insecure=True,
    token="io.deephaven.authentication.psk.PskAuthenticationHandler deephaven",
    metadata={"x-deephaven-auth-cookie-request": "true"},
    middleware=[middleware_factory],
)

flight_info = client.execute("SELECT 42 as Foo")

ticket = flight_info.endpoints[0].ticket

reader = client.do_get(ticket)

pa_table = reader.read_all()
print(pa_table)
client.close()
```

## Query your own tables

Once one of the examples connects to your server, use the same client to query your own Deephaven tables.
For example, create some named Deephaven tables on the server in the web UI:

```python ticking-table order=null
from deephaven import time_table

my_table_1 = (
    time_table("PT1s").view(["Timestamp1 = Timestamp", "Id = ii % 11"]).last_by(["Id"])
)
my_table_2 = (
    time_table("PT5s").view(["Timestamp2 = Timestamp", "Id = ii % 11"]).last_by(["Id"])
)
```

Then replace `SELECT 42 as Foo` in your client example with one of these Flight SQL queries. Because these tables tick, each query returns a snapshot of their rows at the time it runs:

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

## Related documentation

- [Anonymous authentication](./authentication/auth-anon.md)
- [Pre-shared key authentication](./authentication/auth-psk.md)
- [Query scope](./query-scope.md)
- [Table update model](../conceptual/table-update-model.md)
- [Introducing Arrow Flight SQL](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Deephaven Flight SQL examples repository](https://github.com/devinrsmith/deephaven-flightsql-examples)

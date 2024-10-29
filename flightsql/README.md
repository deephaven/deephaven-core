# Flight SQL

See [FlightSqlResolver](src/main/java/io/deephaven/server/flightsql/FlightSqlResolver.java) for documentation on
Deephaven's Flight SQL service.

## Client

The Flight SQL client is simple constructed based on the underlying Flight client.

```java
FlightClient flightClient = ...;
FlightSqlClient flightSqlClient = new FlightSqlClient(flightClient);
```

## JDBC

The default Flight SQL JDBC driver uses cookie authorization; by default, this is not enabled on the Deephaven server.
To enable this, the request header "x-deephaven-auth-cookie-request" must be set to "true".

Example JDBC connection string to self-signed TLS:

```
jdbc:arrow-flight-sql://localhost:8443/?Authorization=Anonymous&useEncryption=1&disableCertificateVerification=1&x-deephaven-auth-cookie-request=true
```

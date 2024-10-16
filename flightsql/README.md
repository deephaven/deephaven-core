# FlightSQL

## Client

## JDBC

The default FlightSQL JDBC driver uses cookie authorization; by default, this is not enabled on the Deephaven server.
To enable this, the request header "x-deephaven-auth-cookie-request" must be set to "true".

Example JDBC connection string to self-signed TLS:

```
jdbc:arrow-flight-sql://localhost:8443/?Authorization=Anonymous&useEncryption=1&disableCertificateVerification=1&x-deephaven-auth-cookie-request=true
```

This directory contains several example authentication providers, which can be added to a deephaven-core classpath
and configured to authenticate the user. 

As these are only examples and not intended to replace actual integrations, they may have some limitations in how
they can be configured and deployed, but also are built as a single `.jar` file with no external dependencies or 
conflicts to manage.

### [Pre-Shared Key](psk)
Allows a key (and user details) to be set via configuration, or a fresh one will be configured on startup. The login
link and details will be logged on startup. 

### [mTLS](mTLS)
With a Mutual-TLS certificate on the client signed by an appropriate trusted Certificate Authority, the user's details
can be read from the certificate and used on the server.

### [Simple PostgreSQL username/password](sql-username-password)
Assumes a PostgreSQL database will be running (and JDBC connection string provided in configuration) with a table
for users and their credentials, along with a table for allowed roles.

### [SAML 2.0](saml)
Given appropriate configuration for an existing Identity Provider (IDP), Deephaven behaves as a Service Provider (SP)
and lets the the user authenticate to gain access to the system.
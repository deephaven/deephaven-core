---
title: Configure username/password authentication
sidebar_label: Username/password
---

This guide will show you how to configure username/password authentication for Deephaven.

Username/password is a common authentication method in which an application can be used by a certain number of users, each with their own username and password. It verifies the identity of a user through the username, and that the user is who they say they are, with the secret password.

> [!WARNING]
> The [example configuration](#example) given in this guide is _not_ recommended for use in a production environment. It is only meant to show a basic implementation that can be used as a template.

## Configuration

Username/password authentication requires some extra configuration on top of Deephaven's basic setup. First and foremost, username/password combinations will be stored in a SQL DB that will be run from Docker alongside the Deephaven application. Additionally, a JAR file that manages the authentication itself will be added to the Deephaven classpath as an extra configuration parameter.

For a basic setup, there are three required files:

- A SQL file to create a database of username/password combinations.
- A Dockerfile that will build the Deephaven image and add the required JAR to the Deephaven classpath.
- A docker-compose file that builds the SQL and Deephaven services.

More advanced setups will build off this basic configuration.

## Example

> [!WARNING]
> This example setup is not recommended for use in a production environment. It is only meant to show a simple configuration with a single user.

### SQL

The following SQL file will create a database with a single table (`users`), in the `deephaven_username_password_auth` schema. It has a single entry for a user named `admin` with the password `p@ssw0rd`.

> [!WARNING]
> The password `p@ssw0rd` does not meet minimum security requirements. It is hashed in the SQL file for protection, but that is not sufficient in the case of an adversarial attack.

```sql
CREATE SCHEMA deephaven_username_password_auth;

CREATE TABLE deephaven_username_password_auth.users (
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    rounds INT NOT NULL
);

INSERT INTO deephaven_username_password_auth.users (username, password_hash, rounds)
VALUES ('admin', '$2a$10$kjbt1Fq4k4W6EB67GDhAauuIWeI8ppx2gsi6.zLL2R5UYokek8nqO', 10);
```

### Dockerfile

The Dockerfile will add a required JAR file to the Deephaven classpath. This JAR file contains all the nuts and bolts necessary for username/password authentication through SQL.

> [!NOTE]
> Make sure that the Deephaven version (0.36.0 below) is the same as the version given in your `docker-compose.yml` file.

```Dockerfile
FROM ghcr.io/deephaven/server:0.36.0
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-sql-username-password-authentication-provider/0.36.0/deephaven-sql-username-password-authentication-provider-0.36.0.jar /apps/libs/
```

### docker-compose.yml

Lastly, create the `docker-compose.yml`. It will create two services: `postgres` and `deephaven`.

> [!WARNING]
> The file below turns [fsync](https://man7.org/linux/man-pages/man2/fsync.2.html) off, which is not recommended. It also sets the SQL container admin password to `password` in plaintext, which does not meet minimum security requirements. See [How to use secrets in Docker Compose](https://docs.docker.com/compose/use-secrets/) to learn more about security best practices with Docker.

```yaml
services:
  postgres:
    image: postgres:15.1
    hostname: postgres
    volumes:
      # This file creates a single deephaven user, with username admin and password p@ssw0rd
      - ./init-users.sql:/docker-entrypoint-initdb.d/init-users.sql
    ports:
      - 5432:5432
    environment:
      # The container requires a admin password - this is unsafe, but usable for testing
      - POSTGRES_PASSWORD=password
    command: postgres -c fsync=off -c synchronous_commit=off
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment: START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.authentication.sql.BasicSqlAuthenticationHandler
```

### Start Deephaven

Start Deephaven with `docker compose up --build`, head to `https://localhost:10000/ide`, and enter the username/password combo to start writing queries.

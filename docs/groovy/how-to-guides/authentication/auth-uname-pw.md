---
title: Configure username/password authentication
---

This guide shows how to configure username/password authentication for Deephaven.

With username/password authentication, each user has their own username and a secret password that proves their identity.

> [!WARNING]
> The [example configuration](#set-up-usernamepassword-authentication) given in this guide is _not_ recommended for use in a production environment. It is only meant to show a basic implementation that can be used as a template. Every user who logs in gets the same SuperUser context. A SuperUser context allows everything the Deephaven APIs allow, so the example handler doesn't limit what any user can do.

## Overview

Username/password authentication requires some extra configuration on top of a [Docker deployment of Deephaven](../../getting-started/docker-install.md). A PostgreSQL database that runs in Docker alongside Deephaven stores usernames and passwords. The database stores each password as a bcrypt hash. Bcrypt is a one-way hashing algorithm.

The Dockerfile adds a JAR (Java archive) file to the Deephaven classpath. The classpath is the list of libraries that the Deephaven server loads at startup. That JAR file handles the authentication.

For a basic setup, there are three required files:

- A SQL file to create a schema and a table of usernames and bcrypt-hashed passwords.
- A Dockerfile that builds the Deephaven image and adds the required JAR to the Deephaven classpath.
- A Docker Compose file that defines the PostgreSQL and Deephaven services.

## Set up username/password authentication

### SQL

Save the following as `init-users.sql` in the same directory as your Dockerfile and `docker-compose.yml`. It creates a schema, `deephaven_username_password_auth`, with a single table (`users`). The table has a single entry for a user named `admin` with the password `p@ssw0rd`.

The `rounds` column records the bcrypt cost factor of each hash. This example uses 10, which the `$2a$10$` prefix of the hash records. The handler doesn't check `rounds` against the hash, so keep the two equal. See [Add more users after startup](#add-more-users-after-startup) for the value to use with a new hash.

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

The Dockerfile adds the JAR file from the Overview to the Deephaven classpath. The Groovy example uses the `server-slim` image, which starts a Groovy console.

> [!NOTE]
> Make sure that the version in the `FROM` image tag matches the version of the authentication-provider JAR in the `ADD` URL. Both use version 42.7 in this example.

```Dockerfile
FROM ghcr.io/deephaven/server-slim:42.7
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-sql-username-password-authentication-provider/42.7/deephaven-sql-username-password-authentication-provider-42.7.jar /apps/libs/
```

### docker-compose.yml

Create `docker-compose.yml` in the same directory as the Dockerfile. The `build: .` line builds the Deephaven image from that Dockerfile. The file defines two services: `postgres` and `deephaven`.

The `deephaven` service's host port is configurable. The settings after the file explain how to change it.

> [!WARNING]
> The file below turns [fsync](https://man7.org/linux/man-pages/man2/fsync.2.html) off, which is not recommended. It also sets the `postgres` service's admin password to `password` in plaintext. See [How to use secrets in Docker Compose](https://docs.docker.com/compose/use-secrets/) to learn more about security best practices with Docker.

```yaml
services:
  postgres:
    image: postgres:15.1
    hostname: postgres
    volumes:
      # This file adds one Deephaven login to the users table: username admin, password p@ssw0rd
      - ./init-users.sql:/docker-entrypoint-initdb.d/init-users.sql
    ports:
      - 5432:5432
    environment:
      # Admin password for the postgres container; see the warning above
      - POSTGRES_PASSWORD=password
    command: postgres -c fsync=off -c synchronous_commit=off
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:10000"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.authentication.sql.BasicSqlAuthenticationHandler -Dauthentication.basic.sql.jdbc.connection=jdbc:postgresql://postgres:5432/postgres -Dauthentication.basic.sql.jdbc.user=postgres -Dauthentication.basic.sql.jdbc.password=password
```

The `START_OPTS` value sets these Java options:

- `-Xmx4g`: The maximum Java Virtual Machine (JVM) heap size.
- `-DAuthHandlers`: The authentication handler to use. Here, it is the SQL handler.
- `-Dauthentication.basic.sql.jdbc.connection`: The JDBC (Java Database Connectivity) URL of the user database. The handler can't log in without it.
- `-Dauthentication.basic.sql.jdbc.user` and `-Dauthentication.basic.sql.jdbc.password`: The database login that the handler uses to read the `users` table.

The JDBC URL uses `postgres`, the Compose service name, because containers reach each other by service name, not `localhost`.

The `deephaven` service also defines these settings:

- `ports`: Maps the host port named by the `DEEPHAVEN_PORT` environment variable (default 10000) to port 10000 in the container.
- `volumes`: Mounts `./data` on the host to `/data` in the container.

### Start Deephaven

Start Deephaven with `docker compose up --build`. Then open `http://localhost:10000/ide` and sign in with the username `admin` and the password `p@ssw0rd` that the [SQL file](#sql) created. If you set `DEEPHAVEN_PORT`, replace `10000` in the URL with that host port.

## Add more users after startup

Each row in the `users` table stores a username, a bcrypt hash of the password, and the `rounds` value. The `rounds` value is the bcrypt cost factor, as a power of two: a cost of 10 runs 2^10 hashing rounds. Set `rounds` to the value in the hash's `$2a$...$` prefix. The examples in this guide use 10.

This step needs git and a Java Development Kit (JDK) on your machine, not only Docker.

To generate a hash, clone the [Deephaven source repository](https://github.com/deephaven/deephaven-core), open a shell at its root, and run the `generatePassword` Gradle task. Replace `newPasswordGoesHere` with the password you want to hash:

```bash
./gradlew :authentication:example-providers:sql-username-password:generatePassword --args=newPasswordGoesHere
```

By default, the `generatePassword` task prints a hash that starts with `$2a$10$`.

PostgreSQL runs `init-users.sql` only when the container starts with an empty data directory, which happens on its first start. To add users, insert new rows into the running database instead of editing the file.

The Compose file above doesn't define a named volume or bind mount for database data, so PostgreSQL stores its data in a Docker-managed anonymous volume. `docker compose down` leaves that volume and its rows on disk, but a subsequent `docker compose up` doesn't automatically reattach it. Add the additional users again in the recreated database.

Open a PostgreSQL command-line shell (`psql`) with `docker compose exec postgres psql -U postgres`. Then run an `INSERT` statement with the hash that the `generatePassword` task printed and the matching `rounds` value:

```sql
INSERT INTO deephaven_username_password_auth.users (username, password_hash, rounds)
VALUES ('newuser', '<hash from generatePassword>', 10);
```

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [Pre-shared key authentication](./auth-psk.md)
- [Anonymous authentication](./auth-anon.md)
- [Keycloak authentication](./auth-keycloak.md)
- [mTLS authentication](./auth-mtls.md)

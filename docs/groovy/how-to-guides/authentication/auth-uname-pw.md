---
title: Configure username/password authentication
---

This guide shows you how to configure username/password authentication for Deephaven.

Username/password is a common authentication method in which each user of an application has their own username and a secret password. The username says who the user is, and the password proves it.

> [!WARNING]
> The [example configuration](#set-up-usernamepassword-authentication) given in this guide is _not_ recommended for use in a production environment. It is only meant to show a basic implementation that can be used as a template. Every user who logs in gets the same SuperUser context. A SuperUser context allows everything the Deephaven APIs allow, so the example handler doesn't limit what any user can do.

## Overview

Username/password authentication needs some extra configuration on top of a [Docker deployment of Deephaven](../../getting-started/docker-install.md). A PostgreSQL database that runs in Docker alongside Deephaven stores usernames and passwords. The database stores each password as a bcrypt hash. Bcrypt is a one-way hashing algorithm.

This guide covers the server setup and logging in from the [JS API](../use-jsapi.md). For a basic setup, there are three required files:

- A SQL file to create a schema and a table of usernames and bcrypt-hashed passwords.
- A Dockerfile that builds the Deephaven image and adds the authentication JAR (a Java archive) to the Deephaven classpath, which is the list of libraries that the server loads at startup.
- A Docker Compose file that defines the PostgreSQL and Deephaven services.

## Set up username/password authentication

### SQL

Save the following as `init-users.sql` in the same directory as your Dockerfile and `docker-compose.yml`. It creates a schema, `deephaven_username_password_auth`, with a single table (`users`). The table has a single entry for a user named `admin` with the password `p@ssw0rd`.

The `rounds` column records the bcrypt cost factor of each hash. The cost factor is the exponent of a power of two that sets how many hashing rounds bcrypt runs. For example, a cost of 10 means 2^10 rounds. This example uses 10, which the `$2a$10$` prefix of the hash records. The handler doesn't check `rounds` against the hash, so keep the two equal. See [Add more users after startup](#add-more-users-after-startup) for the value to use with a new hash.

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

The `deephaven` service's host port is configurable. The `ports` bullet below the Compose file explains how to change it.

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

- `ports`: Maps the host port to port 10000 in the container. The host port comes from the `DEEPHAVEN_PORT` environment variable, which defaults to 10000. To change it, set `DEEPHAVEN_PORT` in your shell, or in a `.env` file next to `docker-compose.yml`, before you run `docker compose up`.
- `volumes`: Mounts `./data` on the host to `/data` in the container.

The `postgres` service keeps its data across restarts. See [Add more users after startup](#add-more-users-after-startup) before you reset it.

### Start Deephaven

Start Deephaven with `docker compose up --build`. Then open `http://localhost:10000/jsapi/authentication/basic.html`. The sample page fills in the `admin` username and the `p@ssw0rd` password from the [SQL file](#sql). Click **Go** to log in. If you set `DEEPHAVEN_PORT`, replace `10000` in the URL with that host port.

## Log in from the JS API

Applications that use the [Deephaven JS API](/core/client-api/javascript/modules/dh.html) log in with a username and password from the `users` table, such as the `admin` login from [Start Deephaven](#start-deephaven). Call [`login`](/core/client-api/javascript/classes/dh.CoreClient.html#login) with `type: dh.CoreClient.LOGIN_TYPE_PASSWORD`, the `username`, and the password in `token`. The code below shows the call. [Use the JS API](../use-jsapi.md) shows how to load the API into a web page.

```javascript
import dh from "http://localhost:10000/jsapi/dh-core.js";

async function main() {
  const client = new dh.CoreClient("http://localhost:10000");
  await client.login({
    type: dh.CoreClient.LOGIN_TYPE_PASSWORD,
    username: "admin",
    token: "p@ssw0rd",
  });
}

main().catch((err) => console.error("Login failed:", err));
```

Replace `http://localhost:10000` with the address of your server, in both the import and the client.

## Add more users after startup

Each row in the `users` table stores a username, a bcrypt hash of the password, and the `rounds` value. Set `rounds` to the value in the hash's `$2a$...$` prefix, as described in [the SQL step](#sql). The examples in this guide use 10.

This step needs git and a Java Development Kit (JDK) on your machine, not only Docker.

To generate a hash, clone the [Deephaven source repository](https://github.com/deephaven/deephaven-core), open a shell at its root, and run the `generatePassword` Gradle task. Replace `newPasswordGoesHere` with the password you want to hash:

```bash
./gradlew :authentication:example-providers:sql-username-password:generatePassword --args='"newPasswordGoesHere"'
```

Keep both layers of quotes. The outer single quotes stop the shell from expanding a `$` in the password. The inner double quotes keep a password that contains spaces together as one argument when Gradle parses `--args`. Without the inner double quotes, Gradle splits a password that contains spaces into several arguments, so the hash doesn't match the password you intended.

By default, the `generatePassword` task prints a hash that starts with `$2a$10$`.

The `postgres` service doesn't mount a volume at its data directory, so Docker stores its data in an anonymous volume that a new container doesn't reattach. `docker compose down` removes the container, so the next `docker compose up` starts an empty database and runs `init-users.sql` again. To keep your users across restarts, use `docker compose stop` and `docker compose start` instead.

PostgreSQL runs `init-users.sql` only when the container starts with an empty data directory, which happens on its first start. To add users, insert new rows into the running database instead of editing the file.

Open a PostgreSQL command-line shell (`psql`) by running `docker compose exec postgres psql -U postgres`. Then run an `INSERT` statement with the hash that the `generatePassword` task printed and the matching `rounds` value:

```sql
INSERT INTO deephaven_username_password_auth.users (username, password_hash, rounds)
VALUES ('newuser', '<hash from generatePassword>', 10);
```

After `docker compose down`, add any users you inserted earlier again.

## Related documentation

- [Use the JS API](../use-jsapi.md)
- [Pre-shared key authentication](./auth-psk.md)
- [Anonymous authentication](./auth-anon.md)
- [Keycloak authentication](./auth-keycloak.md)
- [mTLS authentication](./auth-mtls.md)
- [`login` JS API reference](/core/client-api/javascript/classes/dh.CoreClient.html#login)
- [Install guide for Docker](../../getting-started/docker-install.md)

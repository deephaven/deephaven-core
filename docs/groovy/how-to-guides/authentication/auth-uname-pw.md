---
title: Configure username/password authentication
sidebar_label: Username/password
---

This guide will show you how to configure username/password authentication (sometimes referred to as 'basic authentication') for Deephaven. SQL is used to store the username/password combinations. This guide will set up one `admin` user.

## Setup

Setting up username/password authentication requires the configuration of a SQL database, which will be run by Docker alongside Deephaven. Additionally, a JAR to manage the authentication will be added to the Deephaven classpath, and an extra configuration parameter will be specified in the `docker-compose` YAML file.

### SQL

The following SQL file will create a database with a single table (`users`), in the `deephaven_username_password_auth` schema. It has a single entry for a user named `admin` with the password `p@ssw0rd`.

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

```Dockerfile
FROM ghcr.io/deephaven/server-slim:0.36.0
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-sql-username-password-authentication-provider/0.36.0/deephaven-sql-username-password-authentication-provider-0.36.0.jar /apps/libs
```

### docker-compose.yml

Lastly, create the `docker-compose.yml`. It will create two services: `postgres` and `deephaven`.

> [!WARNING]
> The file below sets the admin password for the SQL database containing login information to `password` for example purposes. We recommend storing passwords more securely. See [Manage sensitive data with Docker secrets](https://docs.docker.com/engine/swarm/secrets/) for more information.

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

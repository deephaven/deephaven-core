---
title: Configure Keycloak for authentication
---

This guide shows you how to configure and use [Keycloak](https://www.keycloak.org/) with [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) to authenticate users running Deephaven from Docker.

Keycloak is an open-source tool for identity and access management that uses realms to create and manage isolated groups of applications and users. Keycloak is too broad a topic to cover in this guide. For more information about Keycloak, see its [documentation](https://www.keycloak.org/documentation).

OpenID Connect (OIDC) is an open authentication protocol that uses an identity provider to verify the identity of users attempting to access a resource. A common example of this type of authentication is signing in to a website with your Google account: the website never sees your password, because Google verifies who you are and tells the website.

Deephaven authenticates users with OIDC through an authentication handler that uses Keycloak as the identity provider. Using it takes a server JAR file, a [web UI plugin](../configuration/js-plugins.md), and some server configuration.

This guide is for administrators who set up Deephaven with Keycloak. If you only need access to a Deephaven instance that uses Keycloak, contact your system administrator. To add users to an existing setup, see [Grant access to users](#grant-access-to-users).

> [!WARNING]
> The [example configuration](#set-up-the-example) in this guide is _not_ recommended for use in a production environment. Use it only as a template for a basic implementation. It differs from a production setup in these ways:
>
> - It creates two users, `admin` and `user`, whose passwords match their usernames.
> - The Keycloak administrator account and the Postgres database also use placeholder passwords.
> - Keycloak runs in development mode (`start-dev`) over plain HTTP.
> - Postgres runs with [`fsync`](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-FSYNC) and `synchronous_commit` turned off. Leave both on in production.

> [!IMPORTANT]
> Deephaven's OIDC authentication handler gives every user who logs in through Keycloak the same SuperUser context, and Deephaven Community allows every logged-in user to do everything the Deephaven APIs allow. This applies to any Keycloak configuration, not only the example. Roles assigned in Keycloak, such as the `user` and `admin` roles in the [example realm](#deephaven_realmjson), don't limit what a user can do in Deephaven.

## Set up the example

The example runs Deephaven, Keycloak, and a Postgres database for Keycloak in Docker.

In Keycloak, a client is the registration for an application that users log in to. In this example, the client is the Deephaven web UI.

Keycloak's realm, its client, and its users come from a JSON file that Docker Compose mounts into the Keycloak container with a [Docker bind mount](../../conceptual/docker-data-volumes.md).

As written, the example works only with Docker Engine running directly on Linux, where the host can reach Docker bridge addresses. On Docker Desktop for macOS, Windows, or Linux, make the two changes in the note at the end of the [docker-compose.yml](#docker-composeyml) section before you build.

Create these files, which the rest of this section describes one at a time:

```
.
├── docker-compose.yml
└── docker
    ├── deephaven
    │   └── Dockerfile
    ├── keycloak
    │   └── deephaven_realm.json
    └── postgres
        └── init-keycloak.sql
```

Docker Compose creates the `data` directory, which the `deephaven` service mounts, the first time you run the example.

### docker-compose.yml

The Docker Compose YAML file sets up three services: `deephaven`, `database`, and `keycloak`. It also sets up a bridge network that connects all three.

<details>
<summary> Expand the YAML file </summary>

```yaml
services:
  deephaven:
    container_name: deephaven
    build:
      context: docker/deephaven
    environment:
      START_OPTS: -Xmx4g
        -DAuthHandlers=io.deephaven.authentication.oidc.OidcAuthenticationHandler
        -Dauthentication.oidc.keycloak.url=http://10.222.1.10:8080
        -Dauthentication.oidc.keycloak.realm=deephaven_core
        -Dauthentication.oidc.keycloak.clientId=deephaven
        -Dauthentication.client.configuration.list=AuthHandlers,authentication.oidc.keycloak.url,authentication.oidc.keycloak.realm,authentication.oidc.keycloak.clientId
    ports:
      - "10000:10000"
    volumes:
      - ./data:/data
    depends_on:
      keycloak:
        condition: service_healthy
    networks:
      - dh_auth_ntw

  database:
    image: postgres:14.2
    container_name: postgres
    hostname: postgres
    volumes:
      - ./docker/postgres/init-keycloak.sql:/docker-entrypoint-initdb.d/init-keycloak.sql:ro
    expose:
      - 5432
    environment:
      - POSTGRES_PASSWORD=password
    command: postgres -c fsync=off -c synchronous_commit=off
    networks:
      - dh_auth_ntw

  keycloak:
    image: quay.io/keycloak/keycloak:26.8.1
    container_name: keycloak
    environment:
      - KC_BOOTSTRAP_ADMIN_USERNAME=admin
      - KC_BOOTSTRAP_ADMIN_PASSWORD=admin
    command:
      - start-dev
      - --db=postgres
      - --db-url-host=postgres
      - --db-url-database=postgres
      - --db-username=postgres
      - --db-password=password
      - --db-schema=keycloak
      - --import-realm
      - --health-enabled=true
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "exec 3<>/dev/tcp/localhost/9000 && printf 'GET /health/ready HTTP/1.0\\r\\nHost: localhost\\r\\n\\r\\n' >&3 && head -1 <&3 | grep -q ' 200 '",
        ]
      interval: 3s
      timeout: 2s
      retries: 60
    ports:
      - "8080:8080"
    volumes:
      - ./docker/keycloak/deephaven_realm.json:/opt/keycloak/data/import/deephaven_realm.json:ro
    depends_on:
      - database
    networks:
      dh_auth_ntw:
        ipv4_address: 10.222.1.10

networks:
  dh_auth_ntw:
    driver: bridge
    ipam:
      config:
        - subnet: 10.222.1.0/24
```

</details>

There's a lot going on in this file. Here's what it all means:

- The `deephaven` service:
  - Builds its image from the Dockerfile in `docker/deephaven`.
  - Gets these settings from [`START_OPTS`](../configuration/docker-application.md#configuration-mechanisms):
    - [`AuthHandlers`](../configuration/configuration-properties.md#authentication-configuration) tells Deephaven to use its OIDC authentication handler in place of the default [pre-shared key handler](./auth-psk.md).
    - `authentication.oidc.keycloak.url`, `authentication.oidc.keycloak.realm`, and `authentication.oidc.keycloak.clientId` give the Keycloak server URL and port, its realm, and the client ID. Both the Deephaven server and the user's browser connect to Keycloak at this URL, so it must be reachable from both.
    - `authentication.client.configuration.list` names the properties the server sends to the web UI before the user logs in. The web UI turns on the Keycloak [plugin](../configuration/js-plugins.md) only when [`AuthHandlers`](../configuration/configuration-properties.md#authentication-configuration) includes the OIDC handler, and the plugin reads the URL, realm, and client ID from the other three properties.
  - Mounts the local `data` directory as `/data`, the Deephaven data directory in the container.
  - Waits to start until the `keycloak` service's health check passes.
- The `database` service runs the [`postgres`](https://hub.docker.com/_/postgres) image:
  - It mounts the SQL file that initializes the Keycloak database.
  - Other containers reach it on port 5432. The port isn't published on the host.
  - `POSTGRES_PASSWORD` sets the database password to `password`.
  - The `command` turns `fsync` and `synchronous_commit` off to speed up the example. The warning at the top of this guide covers these settings in production.
- The `keycloak` service runs the [Keycloak image](https://quay.io/repository/keycloak/keycloak):
  - `KC_BOOTSTRAP_ADMIN_USERNAME` and `KC_BOOTSTRAP_ADMIN_PASSWORD` set the initial Keycloak administrator credentials.
  - The `command` options start Keycloak in development mode with the Postgres database, import the realm file, and turn on Keycloak's health endpoints.
  - The health check queries Keycloak's management port, 9000, every 3 seconds.
  - The service publishes port 8080 on the host.
  - The service mounts the Deephaven realm file into the container read-only.
- All three services join the `dh_auth_ntw` bridge network. The `keycloak` service gets the fixed address `10.222.1.10`, which is the address in `authentication.oidc.keycloak.url`.

> [!IMPORTANT]
> The example works only if your browser can reach the `keycloak` container at `10.222.1.10`, which it can with Docker Engine on Linux. On Docker Desktop, your browser can't reach that container address. To run the example there, make two changes before you [build and run](#build-and-run) it:
>
> - Add `127.0.0.1 keycloak` to your hosts file: `/etc/hosts` on macOS and Linux, or `C:\Windows\System32\drivers\etc\hosts` on Windows.
> - Change `authentication.oidc.keycloak.url` to `http://keycloak:8080`.
>
> Your browser then reaches Keycloak through the published port 8080, and the `deephaven` container reaches it through the Compose network, both under the same name.

### Dockerfile

The `deephaven` service in `docker-compose.yml` builds its image from the `docker/deephaven` directory. Create a file named `Dockerfile` in that directory with the following content:

```dockerfile
ARG VERSION=42.9

FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-auth-keycloak

FROM ghcr.io/deephaven/server:${VERSION}
ARG VERSION
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/${VERSION}/deephaven-oidc-authentication-provider-${VERSION}.jar /apps/libs/
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

The Dockerfile builds on the `server` image, which runs a Python console.

This Dockerfile does three things:

- Packages the Keycloak authentication [JS plugin](../configuration/js-plugins.md).
- Adds the matching [Deephaven OIDC JAR file](https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/) to `/apps/libs`. The server image puts every JAR file in `/apps/libs` on the server's classpath.
- Copies the packaged plugin to `/opt/deephaven/config/js-plugins/`, where the server looks for [JS plugins](../configuration/js-plugins.md).

To use a different Deephaven release, pass it when you build, for example `docker compose build --build-arg VERSION=42.8`. `VERSION` must be a release number such as `42.9`. Maven Central has no `latest` version of the JAR.

> [!IMPORTANT]
> The server image and JAR file _must_ use the same Deephaven version. A version mismatch between them can cause errors when the server starts or when users log in. The `VERSION` argument sets both.

### deephaven_realm.json

Create a file named `deephaven_realm.json` in the `docker/keycloak` directory. It defines the `deephaven_core` realm, the `deephaven` client that the Deephaven web UI uses, and the two users. It should look like this:

<details>
<summary> Expand the JSON file </summary>

```json
{
  "realm": "deephaven_core",
  "enabled": true,
  "users": [
    {
      "username": "user",
      "enabled": true,
      "email": "user@example.com",
      "emailVerified": true,
      "firstName": "Example",
      "lastName": "User",
      "credentials": [
        {
          "type": "password",
          "value": "user"
        }
      ],
      "realmRoles": ["user"]
    },
    {
      "username": "admin",
      "enabled": true,
      "email": "admin@example.com",
      "emailVerified": true,
      "firstName": "Example",
      "lastName": "Admin",
      "credentials": [
        {
          "type": "password",
          "value": "admin"
        }
      ],
      "realmRoles": ["user", "admin"]
    }
  ],
  "roles": {
    "realm": [
      {
        "name": "user",
        "description": "User privileges"
      },
      {
        "name": "admin",
        "description": "Administrator privileges"
      }
    ]
  },
  "clients": [
    {
      "clientId": "deephaven",
      "name": "Deephaven Core",
      "description": "Deephaven Core, a real-time query engine",
      "rootUrl": "",
      "adminUrl": "",
      "baseUrl": "",
      "surrogateAuthRequired": false,
      "enabled": true,
      "publicClient": true,
      "alwaysDisplayInConsole": true,
      "clientAuthenticatorType": "client-secret",
      "redirectUris": [
        "http://127.0.0.1:10000/*",
        "https://127.0.0.1:8443/*",
        "http://localhost:10000/*",
        "https://localhost:8443/*"
      ],
      "webOrigins": ["+"],
      "notBefore": 0,
      "bearerOnly": false,
      "consentRequired": false,
      "standardFlowEnabled": true,
      "implicitFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "serviceAccountsEnabled": false,
      "frontchannelLogout": true,
      "protocol": "openid-connect",
      "attributes": {
        "oidc.ciba.grant.enabled": "false",
        "client.secret.creation.time": "1672520481",
        "backchannel.logout.session.required": "true",
        "backchannel.logout.revoke.offline.tokens": "false",
        "backchannel.logout.url": "",
        "post.logout.redirect.uris": "+",
        "display.on.consent.screen": "false",
        "oauth2.device.authorization.grant.enabled": "false",
        "request.uris": "",
        "consent.screen.text": "",
        "frontchannel.logout.url": "",
        "login_theme": ""
      },
      "authenticationFlowBindingOverrides": {},
      "fullScopeAllowed": true,
      "nodeReRegistrationTimeout": -1,
      "defaultClientScopes": [
        "web-origins",
        "acr",
        "roles",
        "profile",
        "email"
      ],
      "optionalClientScopes": [
        "address",
        "phone",
        "offline_access",
        "microprofile-jwt"
      ],
      "access": {
        "view": true,
        "configure": true,
        "manage": true
      },
      "authorizationServicesEnabled": false
    }
  ]
}
```

</details>

The values that tie this file to the rest of the example are:

- `realm` and the client's `clientId` must match `authentication.oidc.keycloak.realm` and `authentication.oidc.keycloak.clientId` in `docker-compose.yml`.
- `redirectUris` lists the addresses Keycloak may send users back to after login. Include every URL users open Deephaven at.
- `publicClient` is `true` because the web UI logs in from the browser, which can't keep a client secret.
- `standardFlowEnabled` is `true` because the web UI logs in with Keycloak's browser redirect (authorization code) flow.
- `webOrigins` is `["+"]`, which lets the web UI request tokens from Keycloak from every origin in `redirectUris`.

The other client settings come from a Keycloak realm export. Keep them as they are for this example.

Keycloak imports `deephaven_realm.json` only if the `deephaven_core` realm doesn't exist yet. The realm, and any changes you make to it, last as long as the containers do, so they survive stopping with Ctrl+C and running `docker compose up` again. To change the realm while the containers exist, use the Keycloak admin console, as described in [Grant access to users](#grant-access-to-users). To apply an edited `deephaven_realm.json`, run `docker compose down -v` to remove the containers and the Postgres data, then start the example again. Keycloak imports the edited file.

### init-keycloak.sql

Keycloak stores its data, including the realm and its users, in Postgres. Create a file named `init-keycloak.sql` in the `docker/postgres` directory with the following content. It creates the `keycloak` schema that the `keycloak` service's `--db-schema` option names.

```sql
CREATE SCHEMA IF NOT EXISTS keycloak
```

## Build and run

Return to the directory that contains `docker-compose.yml` and run the following commands:

```bash
docker compose build --no-cache
docker compose up
```

The first command builds the `deephaven` image without using cached layers, and the second starts the containers. Docker pulls the Postgres and Keycloak images the first time you start them.

Navigate to `http://localhost:10000/ide`. The web UI redirects you to Keycloak's login page, which prompts for a username and password. Log in as one of the example users: `user` with password `user`, or `admin` with password `admin`. After you log in, Keycloak sends you back to the Deephaven web UI.

When you're done, stop the containers with Ctrl+C. The containers and the Keycloak database remain, so `docker compose up` starts the example again with any users you added.

To remove the containers, run:

```bash
docker compose down -v
```

This also deletes the Postgres data, including users you added in the admin console. The next time you start the example, Keycloak imports the realm file again. Use `-v` here: the Postgres data is in an anonymous volume, so `docker compose down` without it still starts the next run with an empty database and leaves the old volume behind.

## Grant access to users

Users log in to Deephaven with accounts in the Keycloak realm that `authentication.oidc.keycloak.realm` names (`deephaven_core` in the example). To add a user, use the Keycloak admin console:

1. Open the Keycloak admin console at `/admin` on your Keycloak server. In the example, the `keycloak` service publishes port 8080 on the host, so open `http://localhost:8080/admin`.
2. Log in as a Keycloak administrator. In the example, log in as `admin` with password `admin`. The `KC_BOOTSTRAP_ADMIN_*` variables in `docker-compose.yml` create this Keycloak administrator account, which belongs to Keycloak's `master` realm. It is separate from the `admin` user in the `deephaven_core` realm.
3. Switch to your Deephaven realm and create the user.
4. Give the user an email address, a first name, and a last name. Otherwise, Keycloak makes the user fill them in before it completes their first login.
5. Set a password on the user's **Credentials** tab. Turn off **Temporary** unless you want the user to choose a new password when they first log in.

Every user in the realm who can log in gets full access to Deephaven. See the note on SuperUser access at the top of this guide.

For more detail on each step, see [Keycloak's server administration documentation](https://www.keycloak.org/docs/latest/server_admin/).

## Related documentation

- [Access your file system with Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [Install and run with Docker](../../getting-started/docker-install.md)
- [Configure the Deephaven Docker application](../configuration/docker-application.md)
- [Install and use plugins](../install-use-plugins.md)
- [Configure JS plugins](../configuration/js-plugins.md)
- [Configure and use pre-shared key authentication](./auth-psk.md)
- [Configure username/password authentication](./auth-uname-pw.md)
- [Configuration properties reference](../configuration/configuration-properties.md)

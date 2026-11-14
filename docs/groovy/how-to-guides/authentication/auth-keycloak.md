---
title: Configure Keycloak for authentication
---

This guide shows how to configure and use [Keycloak](https://www.keycloak.org/) with [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) to authenticate users running Deephaven from Docker.

Keycloak is an open-source tool for identity and access management that uses realms to create and manage isolated groups of applications and users. Keycloak is too broad a topic to cover in this guide — for more information about Keycloak, see the [Keycloak documentation](https://www.keycloak.org/documentation).

OpenID Connect (OIDC) is an open authentication protocol that uses an authorization server to verify the identity of users attempting to access a resource. A common example of this type of authentication is using a Google account to sign in to a third-party site.

This guide assumes you have [Docker and Docker Compose](../../getting-started/docker-install.md) installed.

> [!NOTE]
> Deephaven servers that use OIDC and Keycloak serve many users. To get access, see [Granting access to users](#granting-access-to-users).

> [!WARNING]
> The [example configuration](#set-up-the-project) in this guide is _not_ recommended for use in a production environment. Use it only as a template for a basic implementation. It differs from a production setup in these ways:
>
> - It creates two users, `admin` and `user`, whose passwords match their usernames.
> - The Keycloak administrator account and the Postgres database also use placeholder passwords.
> - Keycloak runs in development mode (`start-dev`) over plain HTTP.
> - Postgres runs with [`fsync`](https://man7.org/linux/man-pages/man2/fsync.2.html) and `synchronous_commit` turned off. Leave both on in production.

> [!IMPORTANT]
> Deephaven's OIDC authentication handler gives every user who logs in through Keycloak the same `SuperUser` context, and Deephaven Community allows every logged-in user to do everything the Deephaven APIs allow. This applies to any Keycloak configuration, not only the example. Roles assigned in Keycloak, such as the `user` and `admin` roles in the [example realm](#deephaven_realmjson), don't limit what a user can do in Deephaven.

> [!WARNING]
> **Security issue:** The OIDC authentication provider in Deephaven 42.8 and 42.9 has a flaw that lets a login with an invalid token succeed, with full access to the server. The fix is on `main` but isn't in a release yet. The provider JAR that this guide downloads is affected, so until a release includes the fix, don't use the example or the provider to protect a server that untrusted users can reach.

> [!WARNING]
> **Known issue:** In testing with Deephaven 42.8 and 42.9, the web UI doesn't show the Keycloak login screen. The latest published version of the `@deephaven/js-plugin-auth-keycloak` plugin, 0.2.0, fails to load in the web UI, and the IDE shows "No login plugins found". Keycloak and the server's OIDC authentication handler start normally, and users can sign in to Keycloak itself, but until a compatible plugin is published, the example can't sign users in to the IDE.

## Set up the project

The example creates a [Keycloak](https://www.keycloak.org/) realm named `deephaven_core` with the placeholder users described in the production warning at the top of this guide. [Docker Compose](../../getting-started/docker-install.md) runs three services, `deephaven`, `database`, and `keycloak`, and the example uses four files in this layout:

```text
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

### docker-compose.yml

The Docker Compose file defines the three services and a bridge network, `dh_auth_ntw`, that connects all three. The `deephaven` service passes its settings to the server through the [`START_OPTS`](../configuration/docker-application.md#set-jvm-options) environment variable, which appends extra JVM properties to the defaults.

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
      - ./data/deephaven:/data
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

- The `deephaven` service is built from the Dockerfile in `docker/deephaven`, mounts `data/deephaven` as `/data`, the server's data directory, and starts only after the `keycloak` service is healthy.
- The `database` service uses the [`postgres`](https://hub.docker.com/_/postgres) image, mounts the SQL file to initialize the Keycloak database, exposes port 5432 to the other containers without publishing it on the host, and sets the password for the database to `password`.
- The `database` service also turns off `fsync` and `synchronous_commit`.
- The `keycloak` service uses the [Keycloak image](https://quay.io/repository/keycloak/keycloak). It sets the initial admin credentials, runs some commands, creates a health check that queries Keycloak's management port (9000) once per 3 seconds, publishes port 8080 to the host, and mounts the Deephaven realm file into the container as read-only with a [bind mount](../../conceptual/docker-data-volumes.md).
- The `deephaven`, `database`, and `keycloak` services are all attached to the `dh_auth_ntw` network. The `keycloak` service gets the fixed address `10.222.1.10`, which is the address in `authentication.oidc.keycloak.url`.

The `START_OPTS` properties set up the login. The [`AuthHandlers`](../configuration/configuration-properties.md#authentication-configuration) property tells Deephaven to use the OIDC authentication handler in place of the default [pre-shared key handler](./auth-psk.md). The three `authentication.oidc.keycloak.*` properties set the following:

- The Keycloak server URL (`authentication.oidc.keycloak.url`)
- The realm, `deephaven_core` (`authentication.oidc.keycloak.realm`)
- The client ID, `deephaven` (`authentication.oidc.keycloak.clientId`), which is the application that the realm file registers with Keycloak

The `authentication.client.configuration.list` property names the properties that the web UI can read, so the browser knows which Keycloak server and realm to send users to. The [Web Defaults](../configuration/configure-web.md) guide describes the related `client.configuration.list` property, which works the same way for general web UI settings. The web UI turns on the Keycloak [plugin](../configuration/js-plugins.md) only when `AuthHandlers` includes the OIDC handler, and the plugin reads the URL, realm, and client ID from the other three properties.

> [!IMPORTANT]
> The Keycloak URL must be reachable from two places: the `deephaven` container, which reads Keycloak's configuration at startup, and your browser, which is redirected there to sign in. The address `10.222.1.10` is on the Docker network. Your browser can reach it with Docker Engine on Linux, but not on Docker Desktop. To run the example on Docker Desktop:
>
> - Add `127.0.0.1 keycloak` to your hosts file: `/etc/hosts` on macOS and Linux, or `C:\Windows\System32\drivers\etc\hosts` on Windows.
> - Change `authentication.oidc.keycloak.url` to `http://keycloak:8080`.
>
> Your browser then reaches Keycloak through the published port 8080, and the `deephaven` container reaches it through the Compose network, both under the name `keycloak`.

### Dockerfile

The `docker-compose.yml` file in the root directory tells Docker to build images using the Dockerfile found in the `docker/deephaven` directory. So, create that directory, and create a `Dockerfile` in it. It should look like this:

```dockerfile
ARG VERSION=42.8

FROM ghcr.io/deephaven/web-plugin-packager:latest AS js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-auth-keycloak@0.2.0

FROM ghcr.io/deephaven/server-slim:${VERSION}
ARG VERSION
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/${VERSION}/deephaven-oidc-authentication-provider-${VERSION}.jar /apps/libs/
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

The first stage packages the `@deephaven/js-plugin-auth-keycloak` JS plugin, which adds Keycloak sign-in to the web UI. The second stage starts from the `server-slim` image, which omits the Python runtime, and downloads the matching [Deephaven OIDC authentication provider JAR](https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/) into `/apps/libs`, which is on the server's classpath. It then copies the packaged JS plugin into `/opt/deephaven/config/js-plugins/`. For more on how the server loads JS plugins, see [Configure JS plugins](../configuration/js-plugins.md).

To use a different Deephaven release, pass it when building, for example `docker compose build --build-arg VERSION=42.8`. `VERSION` must be a release number such as `42.8`; Maven Central has no `latest` version of the JAR.

> [!IMPORTANT]
> The server image and the JAR file must use the same Deephaven release, or the server and the JAR may not load together. The `VERSION` argument sets both.

### deephaven_realm.json

To configure Keycloak with users and passwords, create a `deephaven_realm.json` file. Place this file in the `docker/keycloak` directory. It should look like this:

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
  "defaultRoles": ["user"],
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

> [!IMPORTANT]
> This file configures the placeholder users described in the production warning at the top of this guide.

The values that tie this file to the rest of the example are:

- `realm` and the client's `clientId` must match `authentication.oidc.keycloak.realm` and `authentication.oidc.keycloak.clientId` in `docker-compose.yml`.
- `redirectUris` lists the addresses Keycloak may send users back to after login. Include every URL users open Deephaven at.
- `publicClient` is `true` because the web UI logs in from the browser, which can't keep a client secret.
- `standardFlowEnabled` is `true` because the web UI logs in with Keycloak's browser redirect (authorization code) flow.
- `webOrigins` is `["+"]`, which lets the web UI request tokens from Keycloak from every origin in `redirectUris`.

The other client settings come from a Keycloak realm export. Keep them as they are for this example.

Keycloak imports `deephaven_realm.json` only if the `deephaven_core` realm doesn't exist yet. The realm, and any changes you make to it, last as long as the containers do. To change the realm while the containers exist, use the Keycloak admin console.

### init-keycloak.sql

The `database` service runs a Postgres database that stores Keycloak's data. Create the file `init-keycloak.sql` in the `docker/postgres` directory with the following SQL. Postgres runs it when it first initializes the database, which creates the `keycloak` schema that the Keycloak service uses.

```sql
CREATE SCHEMA IF NOT EXISTS keycloak;
```

## Build and run

Navigate back to the base directory (which contains `docker-compose.yml`) and run the following commands:

```bash
docker compose build --no-cache
docker compose up
```

The first command builds the Docker images (not using a cache), and the second starts the containers from the built images.

Navigate to `http://localhost:10000/ide`. With a working Keycloak plugin, you're greeted with a login screen prompting for a username and password. Log in with `user`/`user` or `admin`/`admin`. See the known issue at the top of this guide.

When you're done, stop the containers with Ctrl+C. The containers and the Keycloak database remain, so `docker compose up` starts the example again with any users you added.

To remove the containers, run:

```bash
docker compose down -v
```

This also deletes the Postgres data, including users you added in the admin console, so the next run imports the realm file again. Use `-v`: without it, `docker compose down` leaves the Postgres data in an orphaned anonymous volume that the next run doesn't use, so the next run starts with an empty database and imports the realm file again anyway.

## Granting access to users

Most people who use a [Keycloak](https://www.keycloak.org/)-protected Deephaven server don't deploy Keycloak, Postgres, and Deephaven themselves, as the [example](#set-up-the-project) on this page does. Organizations typically have a person or department that manages their application security. Contact your administrator to learn the steps to be granted access to a Deephaven resource guarded by [Keycloak](https://www.keycloak.org/) and [OpenID Connect](https://openid.net/developers/how-connect-works/).

If you are running the example on this page and need to add a user, add a user to the `deephaven_core` realm in the Keycloak admin console:

1. Open the admin console at `http://localhost:8080`. The example publishes this port to the host.
2. Sign in to the master realm with the administrator account. In this example, that account is `admin`/`admin`, which the `KC_BOOTSTRAP_ADMIN_USERNAME` and `KC_BOOTSTRAP_ADMIN_PASSWORD` environment variables define.
3. Switch to the `deephaven_core` realm, create the user, and set a password for them. If you leave the email, first name, or last name empty, Keycloak asks the user to fill them in on an "Update Account Information" page the first time they sign in.

Every user in the realm who can log in gets full access to Deephaven. See the note on `SuperUser` access at the top of this guide.

In a production Keycloak deployment, use your organization's admin console and realm instead.

## Related documentation

- [Configure username/password authentication](./auth-uname-pw.md)
- [Configure mTLS for Deephaven](./auth-mtls.md)
- [Configure JS plugins](../configuration/js-plugins.md)
- [Configure and use pre-shared key authentication](./auth-psk.md)
- [Configuration properties reference](../configuration/configuration-properties.md)
- [Access your file system with Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [How to configure the Docker application](../configuration/docker-application.md)
- [Install guide for Docker](../../getting-started/docker-install.md)
- [Web Defaults](../configuration/configure-web.md)
- [Keycloak server administration documentation](https://www.keycloak.org/docs/latest/server_admin/)

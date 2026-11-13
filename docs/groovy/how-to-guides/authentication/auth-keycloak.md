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
> The [example configuration](#set-up-the-project) given in this guide is _not_ recommended for use in a production environment. For instance, it creates two users: `admin` and `user`, with passwords the same as the usernames. Additionally, the example disables [fsync](https://man7.org/linux/man-pages/man2/fsync.2.html) and `synchronous_commit`, which are not recommended for production. The example is only meant to show a basic implementation that can be used as a template.

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
- The `database` service uses the [`postgres`](https://hub.docker.com/_/postgres) image, mounts the SQL file to initialize the Keycloak database, exposes port 5432, and sets the password for the database to `password`.
- The `database` service also turns off `fsync` and `synchronous_commit`.
- The `keycloak` service uses the [Keycloak image](https://quay.io/repository/keycloak/keycloak). It sets the initial admin credentials, runs some commands, creates a health check that queries Keycloak's management port (9000) once per 3 seconds, publishes port 8080 to the host, and mounts the Deephaven realm file as read-only into the container.
- The `deephaven`, `database`, and `keycloak` services are all attached to the `dh_auth_ntw` network.

The `START_OPTS` properties set up the login. The `AuthHandlers` property tells Deephaven to use the OIDC authentication handler. The three `authentication.oidc.keycloak.*` properties set the following:

- The Keycloak server URL (`authentication.oidc.keycloak.url`)
- The realm, `deephaven_core` (`authentication.oidc.keycloak.realm`)
- The client ID, `deephaven` (`authentication.oidc.keycloak.clientId`), which is the application that the realm file registers with Keycloak

The `authentication.client.configuration.list` property names the properties that the web UI can read, so the browser knows which Keycloak server and realm to send users to. The [Web Defaults](../configuration/configure-web.md) guide describes the related `client.configuration.list` property, which works the same way for general web UI settings.

The Keycloak URL must be reachable from two places. The Deephaven container reads Keycloak's configuration at startup. Your browser is redirected there to sign in. The address `10.222.1.10` is on the Docker network and may not be reachable from your browser, for example on Docker Desktop. If you can't reach it, use a hostname that resolves in both places.

### Dockerfile

The `docker-compose.yml` file in the root directory tells Docker to build images using the Dockerfile found in the `docker/deephaven` directory. So, create that directory, and create a `Dockerfile` in it. It should look like this:

```dockerfile
ARG VERSION=42.8

FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
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

## Granting access to users

Most people who use a [Keycloak](https://www.keycloak.org/)-protected Deephaven server don't deploy Keycloak, Postgres, and Deephaven themselves, as the [example](#set-up-the-project) on this page does. Organizations typically have a person or department that manages their application security. Contact your administrator to learn the steps to be granted access to a Deephaven resource guarded by [Keycloak](https://www.keycloak.org/) and [OpenID Connect](https://openid.net/developers/how-connect-works/).

If you are running the example on this page and need to add a user, add a user to the `deephaven_core` realm in the Keycloak admin console:

1. Open the admin console at `http://localhost:8080`. The example publishes this port to the host.
2. Sign in to the master realm with the administrator account. In this example, that account is `admin`/`admin`, which the `KC_BOOTSTRAP_ADMIN_USERNAME` and `KC_BOOTSTRAP_ADMIN_PASSWORD` environment variables define.
3. Switch to the `deephaven_core` realm, create the user, and set a password for them. If you leave the email, first name, or last name empty, Keycloak asks the user to fill them in on an "Update Account Information" page the first time they sign in.

In a production Keycloak deployment, use your organization's admin console and realm instead.

## Related documentation

- [Configure username/password authentication](./auth-uname-pw.md)
- [Configure mTLS for Deephaven](./auth-mtls.md)
- [Configure JS plugins](../configuration/js-plugins.md)
- [How to configure the Docker application](../configuration/docker-application.md)
- [Install guide for Docker](../../getting-started/docker-install.md)
- [Web Defaults](../configuration/configure-web.md)
- [Keycloak server administration documentation](https://www.keycloak.org/docs/latest/server_admin/)

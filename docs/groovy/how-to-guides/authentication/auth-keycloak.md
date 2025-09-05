---
title: Configure Keycloak for authentication
sidebar_label: Keycloak
---

This guide will show you how to configure and use [Keycloak](https://www.keycloak.org/) with [OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/) to authenticate users running Deephaven from Docker.

Keycloak is a open source tool for identity and access management that uses realms to create and manage isolated groups of applications and users. Keycloak is too broad a topic to cover in this guide - for more information about Keycloak, see its [docs](https://www.keycloak.org/documentation).

OpenId Connect (OIDC) is an open authentication protocol that uses an authorization server to verify the identity of users attempting to access a resource. A common example of this type of authentication is the use of an email to log into a video streaming service.

Configuring Deephaven to use OIDC with Keycloak to guard access requires some configuration. This guide details it.

> [!NOTE]
> This authentication method is typically used by organizations to guard resources accessed by large numbers of people. If you don't need to set up Keycloak, but rather need to gain access or grant access to a Deephaven resource guarded by it, see [Granting access to users](#granting-access-to-users). For more detailed information about Keycloak than this guide provides, see [Keycloak's server administration documentation](https://www.keycloak.org/docs/latest/server_admin/).

## Setup

This guide will first cover the configuration of Keycloak and a database for it.

> [!NOTE]
> The usernames and passwords given in files below are placeholders such as `username`, `admin`, and `password`. This is an unsafe security practice - these placeholders should _never_ be used in a production setup. For more information on best practices to safeguard secrets in Docker, see [Use Secrets in Docker](https://docs.docker.com/compose/use-secrets/).

### docker-compose.yml

The `docker-compose` YAML file will set up three services: `deephaven`, `database`, and `keycloak`. It will also setup the bridge network between Keycloak and the database.

<details>
<summary> Expand the YAML file </summary>

```yaml
services:
  deephaven:
    container_name: deephaven
    build:
      context: docker/deephaven
    environment:
      JAVA_OPTS: -Xmx4g
        -DAuthHandlers=io.deephaven.authentication.oidc.OidcAuthenticationHandler
        -Dauthentication.oidc.keycloak.url=http://10.222.1.10:8080
        -Dauthentication.oidc.keycloak.realm=deephaven_core
        -Dauthentication.oidc.keycloak.clientId=deephaven
        -Dauthentication.client.configuration.list=AuthHandlers,authentication.oidc.keycloak.url,authentication.oidc.keycloak.realm,authentication.oidc.keycloak.clientId
      START_OPS: -Ddeephaven.console.type=python
    ports:
      - "10000:10000"
    volumes:
      - ./data/deephaven:/data/storage
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
    image: quay.io/keycloak/keycloak:19.0.1
    container_name: keycloak
    environment:
      - KEYCLOAK_LOGLEVEL=ALL
      - KEYCLOAK_ADMIN=admin
      - KEYCLOAK_ADMIN_PASSWORD=admin
      - DB_VENDOR=postgres
      - DB_ADDR=postgres
      - DB_DATABASE=postgres
      - DB_USER=postgres
      - DB_SCHEMA=keycloak
      - DB_PASSWORD=password
    command:
      - start-dev
      - --db postgres
      - --db-url-host postgres
      - --db-url-database postgres
      - --db-username postgres
      - --db-password password
      - --db-schema keycloak
      - --import-realm
      - --health-enabled=true
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health/ready"]
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

### Dockerfile

In the `docker-compose.yml` file, it tells Docker to build images using the context found in the `docker/deephaven` directory. So, create that filepath, and create a `Dockerfile`. It should look like this:

```dockerfile
FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-auth-keycloak

FROM ghcr.io/deephaven/server-slim:${VERSION:-latest}
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

RUN wget https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/${VERSION:-latest}/deephaven-oidc-authentication-provider-${VERSION:-latest}.jar -P /apps/libs/
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/
```

This Dockerfile installs the necessary Keycloak packages, builds the latest version of the Deephaven server, and copies the required [Deephaven OIDC JAR file](https://repo1.maven.org/maven2/io/deephaven/deephaven-oidc-authentication-provider/0.36.0/deephaven-oidc-authentication-provider-0.36.0.jar) into the `/apps/libs` directory within the Deephaven Docker container.

> [!NOTE]
> The Deephaven version (`${VERSION:-latest}` in the Dockerfile above) _must_ be consistent between the server and JAR file. Inconsistencies between them will cause errors.

## Keycloak

To configure Keycloak with users and passwords, a `deephaven_realm.json` file must be created. Place this file in the filepath `docker/keycloak`. It should look like this:

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

> [!NOTE]
> This file configures users with passwords that do not meet minimum security requirements. These are placeholders, and should _never_ be used in a production system.

### Database

The last thing to create is a database. In this case, a Postgres DB will be used to store credentials. The following SQL file can be placed within the `docker/postgres` directory.

```sql
CREATE SCHEMA IF NOT EXISTS keycloak
```

### Build and run

Navigate back to the base directory (which contains `docker-compose.yml`) and run the following commands:

```bash
docker compose build --no-cache
docker compose up
```

The first command builds the Docker images (not using a cache), and the second starts the containers from the built images.

Navigate to `http://localhost:10000/ide` and you'll be greeted with a login screen prompting for a username and password. The placeholders in this guide are `username/password` and `admin/admin`.

## Granting access to users

It's unlikely that you'll need to do all of the above setup yourself. Organizations typically have a person or department that manages their application security. Contact your administrator to learn the steps to be granted access to a Deephaven resource guarded by Keycloak and OpenID connect. If you are the security administrator, here's how you can add new users to Deephaven that's guarded with this authentication protocol.

<!-- TODO: Section on adding new users to the Keycloak realm -->

## Related documentation

- [Install guide for Docker](../../tutorials/docker-install.md)
- [How to configure the Docker application](../configuration/docker-application.md)

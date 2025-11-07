# OpenID Connect (via Keycloak)
Simplifying access to services and making resource access uniform usually entails some kind of Single-Sign On feature.
This module, and its related client-side code, enables the use of Keycloak as an OpenID Connect authentication provider,
optionally with roles defined in that service.

Keycloak is too big of a topic to discuss in this documentation, but this module offers a simple Keycloak deployment
and integration to use it from a simple html page.

For the sake of simplicity, the server and client both today assume that the OpenID Connect server is KeyCloak, but the
server component is designed using [Pac4j](https://www.pac4j.org/), making it flexible enough to support not only
general OpenID Connect services, but also other kinds of single-sign on services like SAML, OAuth, etc.

## Example Keycloak setup
In this directory, run `docker compose up` to start Keycloak and database for it. When the container first starts up, a
[JSON file](deephaven_realm.json) configures a realm and a client that can access it, and two users: `admin`/`admin` and
`user`/`user`. This docker compose configuration is designed to be fast to start and ephemeral, and is not meant for
production use.

After that has run, there will be a docker container running with port 6060 exposed on localhost. The account to
administer the keycloak instance is `admin`/`admin`. Note that this account is for the "master" realm, and isn't related
to the admin account above for the "deephaven_core" realm.

## Server setup
To test this when building from gradle with `:server-jetty-app:run`, specify `-Poidc` to enable this.

To use this from a pre-built installation:
* Add the oidc-authentication-provider jar to your Deephaven installation's classpath:
    * If running the `bin/start` script, this can be accomplished by adding it to the `EXTRA_CLASSPATH` environment variable.
    * If running from docker, add the jar to the `/apps/lib/` directory.
* Configure the server to use this by specifying that the `AuthHandlers` configuration should include
  `io.deephaven.authentication.oidc.OidcAuthenticationHandler`. AuthHandlers is a comma-separated string, more than one
  can be specified to enable multiple authentication handlers.
* Configure the other required properties of the Keycloak/OIDC handler:
    * `authentication.oidc.keycloak.url` specifies the URL that the user will use to connect to Keycloak, typically
       http://localhost:6060/.
    * `authentication.oidc.keycloak.realm` specifies the name of the keycloak realm. For the above realm configuration,
      this should be `deephaven_core`.
    * `authentication.oidc.keycloak.clientId` specifies the clientId that the HTML client and Deephaven server should
      use. For the above realm configuration, this should be `deephaven`.

## Testing from the browser
Connect to http://<server-hostname>/jsapi/authentication/oidc.html (the sample username and password are already
populated) and click "Go" to test the connection. Click "Go" again when the page reloads to finish.

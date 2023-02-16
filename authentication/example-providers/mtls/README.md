# Mutual TLS (mTLS)

By requiring not only that the server present a TLS certificate signed by an acceptable CA, but also requiring that
the client do the same, the server and client are able to identify each other during the initial TLS handshake. This
can be tied in to the Deephaven authentication system by asking the client to specify an authorization header with
simply the name of the Authentication Handler in use (in this example, the string `io.deephaven.authentication.mtls.MTlsAuthenticationHandler`).

## Server setup
To test this when building from gradle with `:server-jetty-app:run`, specify `-PdevMTLS` to enable this.

To use this from a pre-built installation:
 * Configure your Deephaven server to support TLS. See [the server instructions on SSL](../../../server/jetty-app/README.md#ssl-examples)
   for more information on this.
 * Next, configure the server to only permit clients to connect if they have a certificate signed by the specified CA.
   See [the server instructions on mTLS](../../../server/jetty-app/README.md#zero-trust--mutual-tls).
 * Add the mtls-authentication-provider jar to your Deephaven installation's classpath:
    * If running the `bin/start` script, this can be accomplished by adding it to the `EXTRA_CLASSPATH` environment variable.
    * If running from docker, add the jar to the `/apps/lib/` directory.
 * Configure the server to use this by specifying that the `AuthHandlers` configuration should include
   `io.deephaven.authentication.mtls.MTlsAuthenticationHandler`. AuthHandlers is a comma-separated string, more than one
   can be specified to enable multiple authentication handlers.

## Testing from the browser
First install a client certificate in the browser. This process depends on the browser in use. Connect to
https://<server-hostname>/jsapi/authentication/mtls.html and click "Go" for a simple test to confirm that the server
accepts the certificate.

## Further customization
Right now, this isn't very extensible, but it is also very simple. The MTlsAuthenticationHandler can be copied and modified
to produce a specfic `AuthContext` instance that will be recognized by any authorization handling present in the system.
# Pre-Shared Key

A pre-shared key is a simple way to easily configure a server and demonstrate that the user who is connecting is permitted
to do so. Most people are familiar with this in the context of using a WiFi password, via WPA-PSK, where every user
enters the same password into all of their devices to connect to the wireless network.

Like WiFi, in the context of Deephaven, this is a simple alternative to permitting any user to anonymously connect and
consume any resources. If enabled with no parameters, the server will generate a new password on each startup and log
it for the user to see. Alternatively, a password can be configured, and this will always be used.

## Server setup
To test this when building from gradle with `:server-jetty-app:run`, specify `-Ppsk` to enable this. To specify a
password, add an equals sign and the desired password, for example `-Ppsk=password-goes-here.

To use this from a pre-built installation:
* Add the psk-authentication-provider jar to your Deephaven installation's classpath:
    * If running the `bin/start` script, this can be accomplished by adding it to the `EXTRA_CLASSPATH` environment variable.
    * If running from docker, add the jar to the `/apps/lib/` directory.
* Configure the server to use this by specifying that the `AuthHandlers` configuration should include
  `io.deephaven.authentication.psk.PskAuthenticationHandler`. AuthHandlers is a comma-separated string, more than one
  can be specified to enable multiple authentication handlers.

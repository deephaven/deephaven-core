# Development Certificates

This are *self-signed* certificates for *local* development purposes.

Generated with the command:

```shell
openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:1024 -keyout key.pem -out ca.crt -sha256 -days 3650
```

The respective servers can be started up locally with these certs via:

```shell
./gradlew server-netty-app:run -Pgroovy -PdevCerts
```

```shell
./gradlew server-jetty-app:run -Pgroovy -PdevCerts
```

From the java client examples, the CA certificate can be referenced via:

```shell
./gradlew java-client-session-examples:installDist
./java-client/session-examples/build/install/java-client-session-examples/bin/<program> --target dh://localhost:8443 --ssl server/dev-certs/client.json --groovy
```

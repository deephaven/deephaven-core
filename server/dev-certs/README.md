# Development Certificates

This are *self-signed* certificates for *local* development purposes.

Self-signed certificates can be generated for server identification with the command:

```shell
openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:1024 -keyout server.key -out server.crt -sha256 -days 3650
```

The respective servers can be started up with development certificates via:

```shell
./gradlew server-netty-app:run -Pgroovy -PdevCerts
```

```shell
./gradlew server-jetty-app:run -Pgroovy -PdevCerts
```

The jetty server will be accessible via https at [https://localhost:8443/ide/](https://localhost:8443/ide/). You'll need
to add the appropriate self-signed certificates to the browser, or proceed past a self-signed warning.

From the java client examples, the server can be connected to via:

```shell
./gradlew java-client-session-examples:installDist
./java-client/session-examples/build/install/java-client-session-examples/bin/<program> --target dh://localhost:8443 --ssl server/dev-certs/client.json --groovy
```

## mTLS

Mutual TLS is authentication in both directions between servers and clients.

The respective servers can be started up with development certificates and mTLS via:

```shell
./gradlew server-netty-app:run -Pgroovy -PdevMTLS
```

```shell
./gradlew server-jetty-app:run -Pgroovy -PdevMTLS
```

Note: it is not possible to start with mTLS, but not certificates.

The jetty server will be accessible via https at [https://localhost:8443/ide/](https://localhost:8443/ide/). You'll need
to import the client.p12 into your browser for mTLS to pass the proper identity information to the server. The
configuration here uses the password `secret`.

From the java client examples, the mTLS configuration can be referenced via:

```shell
./gradlew java-client-session-examples:installDist
./java-client/session-examples/build/install/java-client-session-examples/bin/connect-check --target dh://localhost:8443 --ssl server/dev-certs/client-mtls.json
```
## Configuration

The following are examples of how the keys and certificates in dev-certs was generated.

### CA

```shell
openssl genpkey -algorithm RSA -out ca.key 

openssl req \
  -new \
  -x509 \
  -nodes \
  -days 3650 \
  -subj '/CN=deephaven-localhost-testing-ca' \
  -key ca.key \
  -out ca.crt
```

### Server

```shell
openssl genpkey -algorithm RSA -out server.key 

openssl req \
  -new \
  -key server.key \
  -subj '/CN=localhost' \
  -out server.csr
  
openssl x509 \
  -req \
  -in server.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -days 3650 \
  -out server.crt

rm server.csr

cat server.crt ca.crt > server.chain.crt
```

### Client

```shell
openssl genpkey -algorithm RSA -out client.key 

openssl req \
  -new \
  -key client.key \
  -subj '/CN=insert-name-here/UID=1234/emailAddress=user@deephaven.io/O=my-company' \
  -out client.csr
  
openssl x509 \
  -req \
  -in client.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -days 3650 \
  -out client.crt
  
rm client.csr

cat client.crt ca.crt > client.chain.crt

openssl pkcs12 -export -in client.chain.crt -inkey client.key -out client.p12 -passout pass:secret
```

---
title: Configure mTLS for Deephaven
---

This guide shows you how to configure Deephaven with Docker to use mutual Transport Layer Security (mTLS) to authenticate users trying to gain access.

mTLS is an authentication method in which the server and the client each verify the other's certificate. A certificate authority decides which certificates are valid.

Configuring mTLS takes a few extra steps on top of a basic [Deephaven Docker setup](../../getting-started/docker-install.md). You need the following:

- Certificates for the server and the clients.
- A Dockerfile that adds the required JAR file to the Deephaven classpath. The classpath is the list of JAR files the server loads.
- A docker-compose YAML file that sets the mTLS server properties.

`MTlsAuthenticationHandler` is an example provider. It accepts any client that presented a certificate when it connected, and it uses the certificate's subject (the name the certificate was issued to) only to label the user in log output. It doesn't check which CA signed the certificate. The TLS layer already rejects certificates that `ca.crt` doesn't sign.

## Set up mTLS with Docker

### Certificates

Setting up mTLS requires three files on the server, and each client needs a certificate that `ca.crt` signs. See [Connect with a client certificate](#connect-with-a-client-certificate).

- `server.chain.crt`: The server certificate chain.
- `server.key`: The server private key.
- `ca.crt`: The certificate authority that signs client certificates.

Creating them is outside the scope of this guide. For self-signed certificates, including the development certificates in `server/dev-certs`, see the [development certificates README](https://github.com/deephaven/deephaven-core/blob/main/server/dev-certs/README.md). For certificates that are not self-signed, see your certificate authority's documentation.

Place the three files in `./data/certs`, next to your `docker-compose.yml` file. The compose file mounts `./data` at `/data`, so they appear at `/data/certs` in the container.

To use the development certificates instead, copy `server.chain.crt`, `server.key`, and `ca.crt` from `server/dev-certs` into `./data/certs`. The server then trusts only the `ca.crt` you copy. See [Connect with a client certificate](#connect-with-a-client-certificate).

If you store the files somewhere else, update the paths in the `START_OPTS` value in `docker-compose.yml` to match. The [configuration properties reference](../configuration/configuration-properties.md) lists each property.

Keeping certificates and keys in `/data/certs` isn't the most secure storage method. For more information on best security practices with Docker, see [How to use secrets in Docker Compose](https://docs.docker.com/compose/use-secrets/).

### Dockerfile

First, create a Dockerfile from which Compose builds the Deephaven application. It adds the required JAR file to the Deephaven classpath. Use the same version for the base image and the mTLS provider JAR. The following example Dockerfile uses version 42.7. It uses the `server` image, which runs a Python console.

```Dockerfile
FROM ghcr.io/deephaven/server:42.7
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-mtls-authentication-provider/42.7/deephaven-mtls-authentication-provider-42.7.jar /apps/libs/
```

### docker-compose.yml

Create a docker-compose file that looks like this:

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-8443}:8443"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.authentication.mtls.MTlsAuthenticationHandler -Dssl.identity.type=privatekey -Dssl.identity.certChainPath=/data/certs/server.chain.crt -Dssl.identity.privateKeyPath=/data/certs/server.key -Dssl.trust.type=certs -Dssl.trust.path=/data/certs/ca.crt -Dssl.clientAuthentication=NEEDED -Dhttp.port=8443
```

To add further checks, copy and modify the handler.

There's a lot of text in the `environment`. Here's what it all means:

- `-Xmx4g`: Set the maximum Java Virtual Machine (JVM) heap to 4 GiB.
- `-DAuthHandlers=io.deephaven.authentication.mtls.MTlsAuthenticationHandler`: Tell Deephaven to use mTLS to authenticate users.
- `-Dssl.identity.type=privatekey`: Set the server's TLS identity to a private key and certificate chain.
- `-Dssl.identity.certChainPath=/data/certs/server.chain.crt`: Set the path to the server's certificate chain.
- `-Dssl.identity.privateKeyPath=/data/certs/server.key`: Set the path to the server's private key.
- `-Dssl.trust.type=certs`: Trust the certificates loaded from `ssl.trust.path`.
- `-Dssl.trust.path=/data/certs/ca.crt`: Set the path to the certificate authority that signs client certificates.
- `-Dssl.clientAuthentication=NEEDED`: Require every client to use mTLS with a certificate.
- `-Dhttp.port=8443`: Listen on port 8443.

### Start Deephaven

Start Deephaven with `--build` so Compose rebuilds the image from your Dockerfile:

```bash
docker compose up --build
```

The server is ready when the log shows `Server started on port 8443`. Keep this terminal open, and continue to [Connect with a client certificate](#connect-with-a-client-certificate).

The `docker compose ps` status shows `unhealthy` for this configuration, even though the server runs. The image's health check connects to port 10000 without TLS, and this server accepts only TLS on port 8443. You can ignore the status.

## Connect with a client certificate

Import a client certificate that `ca.crt` signs into your browser. The development certificates include an example: the `client.p12` file in `server/dev-certs`.

The development CA `server/dev-certs/ca.crt` signs `client.p12`. The password for `client.p12` is `secret`.

Import `client.p12` into your browser's certificate store before you open the test page, which the next paragraph describes. When the browser prompts for a password, enter `secret`. The browser then presents this certificate when it connects.

Then open `https://localhost:8443/jsapi/authentication/mtls.html` in your browser and click **Go**. If the server accepts the certificate, the page logs **Success!**.

If the browser warns about the server's certificate, it may not trust the CA that signed `server.chain.crt`. With the development certificates, approve the warning only for `localhost`. Do not add the bundled development CA to your normal browser or OS trust store because its private key is public. For a securely managed CA, you can import its certificate into your browser's trusted certificates.

If the browser reports a TLS or client certificate error, it may not have presented a certificate that `ca.crt` signed. Check that you imported the client certificate and that `ca.crt` is the CA that signed it.

The test page uses the JS API's mTLS login. This guide doesn't cover logging in to the browser-based Deephaven editor (the IDE) with mTLS.

## Related documentation

- [Install guide for Docker](../../getting-started/docker-install.md)
- [Configure and use pre-shared key authentication](./auth-psk.md)
- [Enable anonymous authentication](./auth-anon.md)
- [Configure username/password authentication](./auth-uname-pw.md)
- [Configure Keycloak for authentication](./auth-keycloak.md)
- [Configuration properties](../configuration/configuration-properties.md)

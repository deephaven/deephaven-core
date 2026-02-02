---
title: Configure mTLS for Deephaven
sidebar_label: mTLS
---

Mutual Transport Layer Security (mTLS) is an authentication method that ensures data security through both parties authenticating each other simultaneously. With mTLS, authentication happens through the use of certificates, where a certificate authority determines what certificates are valid, and which are not. Once both parties are verified, the authorization is complete.

Configuring Deephaven to use mTLS to guard against unauthorized access requires a custom setup with a few extra steps.

## Setup

The majority of the setup requires configuration of the Deephaven Docker application.

### Dockerfile

First, create a Dockerfile from which the Deephaven application will be built. It will add the required JAR file to the Deephaven classpath. The following Dockerfile uses deephaven-core version 0.36.0. If you use a different version, specify a different version in your Dockerfile.

```Dockerfile
FROM ghcr.io/deephaven/server-slim:0.36.0
ADD https://repo1.maven.org/maven2/io/deephaven/deephaven-mtls-authentication-provider/0.36.0/deephaven-mtls-authentication-provider-0.36.0.jar /apps/libs/
```

### docker-compose.yml

Next, create a docker-compose file. It will look like this:

```yaml
services:
  deephaven:
    build: .
    ports:
      - "${DEEPHAVEN_PORT:-10000}:8443"
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.authentication.mtls.MTlsAuthenticationHandler -Dssl.identity.type=privatekey -Dssl.identity.certChainPath=/data/certs/server.chain.crt -Dssl.identity.privateKeyPath=/data/certs/server.key -Dssl.trust.type=certs -Dssl.trust.path=/data/certs/ca.crt -Dssl.clientAuthentication=NEEDED -Dhttp.port=8443
```

There's a lot of text in the `environment`. Here's what it all means:

- `-Xmx4g`: Give Deephaven 4GB of memory.
- `-DAuthHandlers=io.deephaven.authentication.mtls.MTlsAuthenticationHandler`: Tell Deephaven that it will use mTLS to authenticate users.
- `-Dssl.identity.type=privatekey`: A private key will be used to verify identity.
- `-Dssl.identity.certChainPath=/data/certs/server.chain.crt`: The cert chain path can be found in `/data/certs`.
- `-Dssl.identity.privateKeyPath=/data/certs/server.key`: The private key can be found at `/data/certs/server.key`.
- `-Dssl.trust.type=certs`: Certificates are used.
- `-Dssl.trust.path=/data/certs/ca.crt`: The certificate authority is found at `/data/certs/ca.crt`.
- `-Dssl.clientAuthentication=NEEDED`: Any client trying to connect **must** use mTLS with a certificate to do so.
- `-Dhttp.port=8443`: The authentication process happens over port 8443.

Keeping certificates and keys in `/data/certs` isn't the most secure storage method. For more information on best security practices with Docker, see [Manage sensitive data with Docker secrets](https://docs.docker.com/engine/swarm/secrets/).

### Required files

There are several required files listed above. These files are keys, certificates, and more. Creating them is a topic outside the coverage of this document. For more information on creating self-signed certificates, see this [README](https://github.com/deephaven/deephaven-core/blob/main/server/dev-certs/README.md). For more information on creating certificates that are not self-signed, use an external resource.

Place these files within `/data/certs`, or wherever within the Deephaven container you've chosen to place them.

### Start Deephaven

Since Deephaven is being built from an image rather than pulled directly, start Deephaven as such:

```bash
docker compose up --build
```

Navigate to `https://localhost:8443` in your browser. If you're met with a warning about security, it's because you're using self-signed certificates. You can click to approve or switch to using certificates accepted by your browser.

## Related documentation

- [Quickstart for Docker](../../getting-started/docker-install.md)
- [How to configure pre-shared key](./auth-psk.md)
- [How to configure anonyous authentication](./auth-anon.md)

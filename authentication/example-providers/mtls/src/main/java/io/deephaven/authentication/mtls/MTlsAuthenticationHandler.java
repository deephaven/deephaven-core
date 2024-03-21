//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.mtls;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.base.log.LogOutput;
import io.deephaven.grpc.MTlsCertificate;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

public class MTlsAuthenticationHandler implements AuthenticationRequestHandler {
    @Override
    public String getAuthType() {
        // We still need a handshake to create the session, so return a name here that clients can specify.
        // Alternatively, clients could still have an appropriate mtls cert, but use a different means to authenticate.
        return getClass().getName();
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException {
        return checkClientCertificates();
    }

    @NotNull
    private Optional<AuthContext> checkClientCertificates() {
        List<X509Certificate> x509Certificates = MTlsCertificate.CLIENT_CERTIFICATES.get();
        if (x509Certificates != null && !x509Certificates.isEmpty()) {
            // Here we should validate that they were signed by the appropriate CA, or match expected
            // known public keys, etc. If we fail that check return empty or throw an exception.

            return Optional.of(new AuthContext() {
                @Override
                public LogOutput append(LogOutput logOutput) {
                    return logOutput.append(x509Certificates.get(0).getSubjectDN().getName());
                }
            });
        }
        return Optional.empty();
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException {
        return checkClientCertificates();
    }

    @Override
    public void initialize(String targetUrl) {

    }
}

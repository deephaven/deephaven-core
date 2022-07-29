/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.auth;

import com.google.protobuf.ByteString;
import io.deephaven.grpc.MTlsCertificate;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import java.security.cert.X509Certificate;
import java.util.List;

public class TrivialAuthContextProvider implements AuthContextProvider {
    @Inject()
    public TrivialAuthContextProvider() {}

    @Override
    public boolean supportsProtocol(final long authProtocol) {
        return authProtocol == 1;
    }

    @Override
    public AuthContext authenticate(final long protocolVersion, final ByteString payload) {
        List<X509Certificate> x509Certificates = MTlsCertificate.CLIENT_CERTIFICATES.get();
        if (x509Certificates != null && !x509Certificates.isEmpty()) {
            System.out.println(x509Certificates);
            return new AuthContext.SuperUser();
        }
        if (!supportsProtocol(protocolVersion)) {
            return null;
        }

        return new AuthContext.SuperUser();
    }
}

package io.deephaven.authentication.mtls;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;

import java.nio.ByteBuffer;
import java.util.Optional;

public class MTlsAuthenticationHandler implements AuthenticationRequestHandler {
    @Override
    public String getAuthType() {
        return null;
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException {
        return Optional.empty();
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException {
        return Optional.empty();
    }

    @Override
    public void initialize(String targetUrl) {

    }
}

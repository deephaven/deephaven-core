package io.deephaven.server.auth;

import com.google.protobuf.ByteString;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public class TrivialAuthContextProvider implements AuthContextProvider {
    @Inject()
    public TrivialAuthContextProvider() {}

    @Override
    public boolean supportsProtocol(final long authProtocol) {
        return authProtocol == 1;
    }

    @Override
    public AuthContext authenticate(final long protocolVersion, final ByteString payload) {
        if (!supportsProtocol(protocolVersion)) {
            return null;
        }

        return new AuthContext.SuperUser();
    }
}

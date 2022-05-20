package io.deephaven.server.auth;

import com.google.protobuf.ByteString;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.auth.AuthContext;
import io.deephaven.util.auth.AuthContext.SuperUser;

import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;

public class TrivialAuthContextProvider implements AuthContextProvider {

    // TODO: this isn't final configuration, just an example configuration
    private static final String USER_CLASS = Configuration.getInstance()
            .getStringWithDefault("TrivialAuthContextProvider.class", SuperUser.class.getName());

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
        try {
            return (AuthContext) Class.forName(USER_CLASS).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

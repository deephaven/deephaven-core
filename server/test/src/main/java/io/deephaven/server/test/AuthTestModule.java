/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.test;

import dagger.Module;
import dagger.Provides;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.auth.AuthContext;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Module()
public class AuthTestModule {
    public static class BasicAuthTestImpl implements BasicAuthMarshaller.Handler {
        public Map<String, String> validLogins = new HashMap<>();

        @Override
        public Optional<AuthContext> login(String username, String password) throws AuthenticationException {
            final String validPassword = validLogins.get(username);
            if (Objects.equals(validPassword, password)) {
                return Optional.of(new AuthContext.SuperUser());
            }
            return Optional.empty();
        }
    }

    @Provides
    @Singleton
    public BasicAuthTestImpl bindBasicAuthTestImpl() {
        return new BasicAuthTestImpl();
    }

    @Provides
    @Singleton
    public BasicAuthMarshaller bindBasicAuth(BasicAuthTestImpl handler) {
        return new BasicAuthMarshaller(handler);
    }

    @Provides
    @Singleton
    public Optional<BasicAuthMarshaller> bindBasicAuthProvider(BasicAuthMarshaller marshaller) {
        return Optional.of(marshaller);
    }

    @Provides
    @Singleton
    public Map<String, AuthenticationRequestHandler> bindAuthHandlerMap() {
        // note this is mutable
        return new HashMap<>();
    }
}

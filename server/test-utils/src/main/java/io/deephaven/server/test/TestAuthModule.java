//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.test;

import dagger.Module;
import dagger.Provides;
import io.deephaven.auth.AnonymousAuthenticationHandler;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.auth.AuthContext;
import org.apache.arrow.flight.auth2.Auth2Constants;

import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Module
public class TestAuthModule {
    public static class BasicAuthTestImpl implements BasicAuthMarshaller.Handler {
        public Map<String, String> validLogins = new HashMap<>();

        @Override
        public Optional<AuthContext> login(String username, String password) {
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
    public Map<String, AuthenticationRequestHandler> bindAuthHandlerMap(BasicAuthMarshaller basicAuthMarshaller) {
        Map<String, AuthenticationRequestHandler> map = new HashMap<>();
        map.put(basicAuthMarshaller.getAuthType(), basicAuthMarshaller);
        AnonymousAuthenticationHandler anonymous = new AnonymousAuthenticationHandler();
        map.put(anonymous.getAuthType(), anonymous);
        map.put(FakeBearer.INSTANCE.getAuthType(), FakeBearer.INSTANCE);
        return Collections.unmodifiableMap(map);
    }

    public enum FakeBearer implements AuthenticationRequestHandler {
        INSTANCE;

        public static final String TOKEN = UUID.randomUUID().toString();

        @Override
        public String getAuthType() {
            return Auth2Constants.BEARER_PREFIX.trim();
        }

        @Override
        public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload,
                HandshakeResponseListener listener) {
            return Optional.empty();
        }

        @Override
        public Optional<AuthContext> login(String payload, MetadataResponseListener listener) {
            if (payload.equals(TOKEN)) {
                return Optional.of(new AuthContext.SuperUser());
            }
            return Optional.empty();
        }

        @Override
        public void initialize(String targetUrl) {
            // do nothing
        }
    }
}

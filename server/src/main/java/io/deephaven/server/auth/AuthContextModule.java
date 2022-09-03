/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.auth;

import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AnonymousAuthenticationHandler;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.barrage.BarrageStreamGenerator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Module()
public class AuthContextModule {
    private static final Logger log = LoggerFactory.getLogger(BarrageStreamGenerator.class);
    private static final String[] AUTH_HANDLERS = Configuration.getInstance()
            .getStringArrayFromPropertyWithDefault("AuthHandlers",
                    new String[] {AnonymousAuthenticationHandler.class.getCanonicalName()});

    private static BasicAuthMarshaller basicAuthMarshaller;
    private static Map<String, AuthenticationRequestHandler> authHandlerMap;

    private static void initializeAuthHandlers() {
        if (authHandlerMap != null) {
            return;
        }

        authHandlerMap = new LinkedHashMap<>();

        for (String handler : AUTH_HANDLERS) {
            try {
                final Class<?> clazz = AuthContextModule.class.getClassLoader().loadClass(handler);
                Object instance = clazz.getDeclaredConstructor().newInstance();
                if (instance instanceof BasicAuthMarshaller.Handler) {
                    if (basicAuthMarshaller != null) {
                        throw new UncheckedDeephavenException("Found multiple BasicAuthMarshaller.Handlers");
                    }
                    basicAuthMarshaller = new BasicAuthMarshaller((BasicAuthMarshaller.Handler) instance);
                    authHandlerMap.put(basicAuthMarshaller.getAuthType(), basicAuthMarshaller);
                } else if (instance instanceof AuthenticationRequestHandler) {
                    final AuthenticationRequestHandler authHandler = (AuthenticationRequestHandler) instance;
                    authHandlerMap.put(authHandler.getAuthType(), authHandler);
                } else {
                    log.error().append("Provided auth handler does not implement an auth interface: ")
                            .append(handler).endl();
                    throw new UncheckedDeephavenException("Could not load authentication");
                }
            } catch (Exception error) {
                log.error().append("Could not load authentication handler class: ").append(handler).endl();
                log.error().append(error).endl();
                throw new UncheckedDeephavenException(error);
            }
        }

        authHandlerMap = Collections.unmodifiableMap(authHandlerMap);
    }

    @Provides
    public Optional<BasicAuthMarshaller> bindBasicAuth() {
        initializeAuthHandlers();
        return Optional.ofNullable(basicAuthMarshaller);
    }

    @Provides
    public Map<String, AuthenticationRequestHandler> bindAuthHandlerMap() {
        initializeAuthHandlers();
        return authHandlerMap;
    }
}

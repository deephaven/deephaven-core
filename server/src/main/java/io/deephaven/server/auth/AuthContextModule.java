/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.auth;

import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Module
public class AuthContextModule {
    private static final Logger log = LoggerFactory.getLogger(AuthContextModule.class);
    private static final String[] AUTH_HANDLERS =
            Configuration.getInstance().getStringArrayFromProperty("AuthHandlers");

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
                    // Re-wrap this special case in a general auth request handler
                    instance = new BasicAuthMarshaller((BasicAuthMarshaller.Handler) instance);
                }
                if (instance instanceof AuthenticationRequestHandler) {
                    final AuthenticationRequestHandler authHandler = (AuthenticationRequestHandler) instance;
                    AuthenticationRequestHandler existing = authHandlerMap.put(authHandler.getAuthType(), authHandler);
                    if (existing != null) {
                        log.error().append("Multiple handlers registered for authentication type ")
                                .append(existing.getAuthType()).end();
                        throw new UncheckedDeephavenException(
                                "Multiple authentication handlers registered for type " + existing.getAuthType());
                    }
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
    public Map<String, AuthenticationRequestHandler> bindAuthHandlerMap() {
        initializeAuthHandlers();
        return authHandlerMap;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.grpc.UserAgentContext;
import io.grpc.Context;

import javax.inject.Inject;

/**
 * Logs {@link SessionState#getSessionId() session ID} and {@link UserAgentContext#get(Context) user-agent} on session
 * creation. The configuration property "SessionListenerLogger.level" can be set to change the logging level, by default
 * is {@link LogLevel#INFO}.
 */
public final class SessionListenerLogger implements SessionListener {
    private static final Logger log = LoggerFactory.getLogger(SessionListenerLogger.class);
    private final LogLevel level;

    @Inject
    public SessionListenerLogger() {
        level = LogLevel.valueOf(Configuration.getInstance().getStringForClassWithDefault(SessionListenerLogger.class,
                "level", LogLevel.INFO.getName()));
    }

    @Override
    public void onSessionCreate(SessionState session) {
        log.getEntry(level)
                .append("onSessionCreated, id=")
                .append(session.getSessionId())
                .append(", userAgent=")
                .append(UserAgentContext.get(Context.current()).orElse(null))
                .endl();
    }
}

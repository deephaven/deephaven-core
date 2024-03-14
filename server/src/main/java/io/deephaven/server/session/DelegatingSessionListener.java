//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.Collection;

/**
 * Utility class to fan out session events to multiple handlers.
 */
public class DelegatingSessionListener implements SessionListener {
    private static final Logger log = LoggerFactory.getLogger(DelegatingSessionListener.class);
    private final Collection<SessionListener> sessionListeners;

    public DelegatingSessionListener(Collection<SessionListener> sessionListeners) {
        this.sessionListeners = sessionListeners;
    }

    @Override
    public void onSessionCreate(SessionState session) {
        for (SessionListener listener : sessionListeners) {
            try {
                listener.onSessionCreate(session);
            } catch (Exception e) {
                log.error().append("Error invoking session listener ").append(listener.getClass().getName()).append(e)
                        .endl();
            }
        }
    }
}

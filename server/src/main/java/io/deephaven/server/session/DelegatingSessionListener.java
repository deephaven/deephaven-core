package io.deephaven.server.session;

import java.util.Collection;

/**
 * Utility class to fan out session events to multiple handlers.
 */
public class DelegatingSessionListener implements SessionListener {
    private final Collection<SessionListener> sessionListeners;

    public DelegatingSessionListener(Collection<SessionListener> sessionListeners) {
        this.sessionListeners = sessionListeners;
    }

    @Override
    public void onSessionCreate(SessionState session) {
        for (SessionListener listener : sessionListeners) {
            listener.onSessionCreate(session);
        }
    }

    @Override
    public void onSessionRefresh(SessionState session) {
        for (SessionListener listener : sessionListeners) {
            listener.onSessionRefresh(session);
        }
    }

    @Override
    public void onSessionEnd(SessionState session) {
        for (SessionListener listener : sessionListeners) {
            listener.onSessionEnd(session);
        }
    }
}

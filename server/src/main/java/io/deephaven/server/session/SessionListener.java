//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

/**
 * Callbacks for the SessionService, to observe session lifecycles.
 */
public interface SessionListener {
    /**
     * When a new session is created and has been given a refresh token, this will be invoked.
     * <p>
     * </p>
     * To track a session closing, use {@link SessionState#addOnCloseCallback(java.io.Closeable)}.
     *
     * @param session the newly created session
     */
    void onSessionCreate(SessionState session);
}

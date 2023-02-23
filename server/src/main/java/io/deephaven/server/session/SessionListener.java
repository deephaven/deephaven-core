package io.deephaven.server.session;

/**
 * Callbacks for the SessionService, to observe session lifecycles.
 */
public interface SessionListener {
    /**
     * When a new session is created and has been given a refresh token, this will
     * be invoked.
     *
     * @param session the newly created session
     */
    void onSessionCreate(SessionState session);

    /**
     * After a session token is rotated for any reason, this will be invoked.
     *
     * @param session the session with its refreshed token
     */
    void onSessionRefresh(SessionState session);

    /**
     * Before a session is ended for any reason, this will be invoked.
     *
     * @param session the session that is about to end
     */
    void onSessionEnd(SessionState session);
}

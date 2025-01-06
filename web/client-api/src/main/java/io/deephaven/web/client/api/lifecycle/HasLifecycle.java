//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.lifecycle;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.fu.JsLog;

import static io.deephaven.web.client.api.JsTable.EVENT_RECONNECTFAILED;

/**
 * An abstraction over simple lifecycle methods, to see if objects are currently connected to their server-side
 * counterparts.
 */
public abstract class HasLifecycle extends HasEventHandling {
    /**
     * Simple flag to see if already connected - that is, if true, will not fire a reconnect event unless disconnected
     * again.
     */
    private boolean connected = true;

    /**
     * Signal that reconnect was attempted and failed due to differences between client and server state.
     */
    public void die(Object error) {
        JsLog.debug("Die!", this, error);
        unsuppressEvents();
        fireEvent(EVENT_RECONNECTFAILED, error);
        suppressEvents();
    }

    /**
     * Mark the object as being connected to its corresponding server-side object.
     */
    public void reconnect() {
        connected = true;
        unsuppressEvents();
        fireEvent(JsTable.EVENT_RECONNECT);
    }

    /**
     * Indicate that a new session has been created on the server, and this object should re-create its corresponding
     * server-side object if possible. Override this to implement custom behavior, being sure to call reconnect() when
     * finished.
     *
     * @return a promise that will resolve when this object is reconnected
     */
    public Promise<?> refetch() {
        reconnect();
        return Promise.resolve(this);
    }

    /**
     * Mark the object as (still) disconnected from its corresponding server-side object, but reconnect will be
     * attempted in the future.
     */
    public void disconnected() {
        connected = false;
        fireEvent(JsTable.EVENT_DISCONNECT);
        suppressEvents();
    }

    /**
     * Resolves immediately if connected, otherwise will resolve the next time this object is marked as connected, or
     * reject if it can't connect.
     */
    public Promise<Void> nextReconnect() {
        if (connected) {
            return Promise.resolve((Void) null);
        }
        return new Promise<>((resolve, reject) -> {
            addEventListenerOneShot(
                    HasEventHandling.EventPair.of(JsTable.EVENT_RECONNECT, event -> resolve.onInvoke((Void) null)),
                    HasEventHandling.EventPair.of(JsTable.EVENT_DISCONNECT,
                            event -> reject.onInvoke(event.getDetail())),
                    HasEventHandling.EventPair.of(EVENT_RECONNECTFAILED,
                            event -> reject.onInvoke(event.getDetail())));
        });
    }
}

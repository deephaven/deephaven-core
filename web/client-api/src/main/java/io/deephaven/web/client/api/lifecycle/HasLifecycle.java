package io.deephaven.web.client.api.lifecycle;

import elemental2.dom.CustomEventInit;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.state.ClientTableState;

import static io.deephaven.web.client.api.JsTable.EVENT_RECONNECTFAILED;

/**
 * An abstraction over the lifecycle methods used to reconnect JsTable, so we can have non-JsTable
 * bound states get the same revivification treatment.
 */
public interface HasLifecycle {

    // extend HasEventHandling to get these three provided for you.
    void suppressEvents();

    void unsuppressEvents();

    boolean isSuppress();

    void revive(ClientTableState state);

    /**
     * You should probably just call notifyDeath(this, failure), assuming you implement in an object
     * that HasEventHandlers.
     */
    void die(Object failure);

    default void notifyDeath(HasEventHandling handler, Object error) {
        JsLog.debug("Die!", this, error);
        final CustomEventInit init = CustomEventInit.create();
        init.setDetail(error);
        unsuppressEvents();
        handler.fireEvent(EVENT_RECONNECTFAILED, init);
        suppressEvents();
    }

    default void maybeRevive(ClientTableState state) {
        if (isSuppress()) {
            revive(state);
        }
    }

    void disconnected();

    default void notifyDisconnect(HasEventHandling handler) {
        handler.fireEvent(JsTable.EVENT_DISCONNECT);
        suppressEvents();
    }
}

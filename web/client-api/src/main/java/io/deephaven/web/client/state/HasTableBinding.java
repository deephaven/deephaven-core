package io.deephaven.web.client.state;

import elemental2.dom.CustomEventInit;
import io.deephaven.web.client.api.TableTicket;

/**
 * In order to not-require a JsTable to be bound to a {@link ClientTableState}, we will use this interface, which
 * exposes the parts of JsTable that we require in managing handles and their lifecycles.
 *
 */
public interface HasTableBinding {

    TableTicket getHandle();

    boolean hasHandle(TableTicket tableHandle);

    // Leaving this method named state() for now to avoid merge conflicts with myself later.
    // TODO: rename to getState() when the cost to refactor is lower
    // IDS-3078
    ClientTableState state();

    boolean isAlive();

    void fireEvent(String name);

    void fireEvent(String name, CustomEventInit eventInit);

    void setState(ClientTableState appendTo);

    void setRollback(ActiveTableBinding rollbackTo);

    void rollback();

    void maybeReviveSubscription();
}

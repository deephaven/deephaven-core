package io.deephaven.web.client.state;

import io.deephaven.web.client.api.state.HasTableState;

/**
 * Represents a previously active binding that we're still keeping around for a bit.
 *
 * We will mark/sweep all paused bindings at once to decide when to release an item. Since we give
 * requests a crazy ten minute timeout (seriously, if we actually need that long, we should have a
 * long-running-request option that sends back a "work token" where we will notify you of success
 * later.
 */
public class PausedTableBinding implements HasTableState<ClientTableState> {

    private final ActiveTableBinding active;

    public PausedTableBinding(ActiveTableBinding active) {
        this.active = active;
    }

    @Override
    public ClientTableState getState() {
        return active.getState();
    }

    public ActiveTableBinding getActiveBinding() {
        return active;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public void rollback() {
        active.rollback();
    }
}

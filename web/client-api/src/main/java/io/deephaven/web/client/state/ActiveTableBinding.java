package io.deephaven.web.client.state;

import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.state.HasTableState;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.Viewport;

/**
 * An active binding describes the link between a {@link JsTable} and the {@link ClientTableState} it is currently
 * using.
 *
 * Each JsTable can only have one ActiveTableBinding; in order to get a new one, you must "lose" your current one (note
 * the private constructor).
 *
 * This allows us to control transitions to/from an active state.
 *
 * Currently, the new state is created and a new binding built off of it, then the old binding is paused.
 *
 * Equality semantics of this object are based solely on the JsTable object identity.
 *
 * Instances of these objects start life in the constructor of {@link JsTable}. From there, the only way to get a new
 * one is to tell the old one to change state.
 *
 */
public class ActiveTableBinding implements HasTableState<ClientTableState> {

    /**
     * The table that owns this instance.
     *
     * We use object identity semantics of JsTable to determine equality.
     */
    private final JsTable table;

    /**
     * The state that this binding maps to.
     *
     * This state object will keep track of all bindings mapped to it.
     */
    private final ClientTableState state;

    /**
     * This instance's one and only "paused form". We tightly control instances of these, so you can put them into a
     * JsMap / IdentityHashMap.
     */
    private final PausedTableBinding paused;

    // Mutable fields

    /**
     * Our source state in a "paused form".
     *
     * Note that this is "paused with respect to a given table"; a given {@link ClientTableState} can have many
     * concurrent Paused|ActiveTableBindings.
     *
     * Note that all paused|active connected bindings should share the same table. When a new table copy is made, the
     * new table must copy all bindings to the new table.
     */
    private PausedTableBinding rollback;
    private Viewport viewport;
    private RangeSet rows;
    private Column[] columns;
    private boolean subscriptionPending;

    private ActiveTableBinding(
            JsTable table,
            ClientTableState state) {
        this.table = table;
        this.state = state;
        paused = new PausedTableBinding(this);
    }

    public ActiveTableBinding changeState(ClientTableState newState) {
        HasTableState<ClientTableState> existing = newState.getBinding(table);
        if (newState == this.state) {
            if (existing.isActive()) {
                assert this == existing : "Multiple bindings found for " + newState + " and " + table;
            } else {
                assert paused == existing : "Multiple bindings found for " + newState + " and " + table;
                newState.unpause(table);
            }
            return this;
        }
        final boolean isFirstTime = existing == null;
        final boolean thisStateActive = this.state.getBinding(table).isActive();
        final boolean newStateActive = !isFirstTime && existing.isActive();

        // user requested a different state than us for this table.
        assert thisStateActive : "Non-active state cannot change a client's state";
        assert !newStateActive : "Table cannot have two active states at once.";
        // pause this state
        this.state.pause(table);

        // start new state
        ActiveTableBinding newSub;
        if (isFirstTime) {
            newSub = newState.createBinding(table);
            // the new state should roll back to us if it fails.
            // we only ever set the rollback once, when creating a new binding for a given table.
            // this way, if we are changing _back_ to a state (rolling back) we do not overwrite
            // the correct rollback with a node from the future.
            if (!newState.isRunning()) {
                // no need to wire up a rollback if we are moving to a finished state
                if (getState().isRunning()) {
                    // use our paused state if we are running
                    newSub.setRollback(paused);
                } else {
                    // otherwise delegate to our rollback
                    newSub.setRollback(getRollback().getPaused());

                }
            }
        } else {
            newSub = ((PausedTableBinding) existing).getActiveBinding();
            newState.unpause(table);
        }
        return newSub;
    }

    public JsTable getTable() {
        return table;
    }

    @Override
    public ClientTableState getState() {
        return state;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        // we are purposely using object identity of JsTable instance here...
        // we don't maintain an internal identity field for JsTable,
        // so we wind up using the object itself in map keys,
        // ensure we pin lots of memory even if client code would have GC'ed. :'(
        return getClass() == o.getClass() &&
                (table == ((ActiveTableBinding) o).table);
    }

    @Override
    public int hashCode() {
        return table != null ? table.hashCode() : 0;
    }

    public void setRollback(PausedTableBinding rollback) {
        assert rollback == null || rollback.getState().getResolution() == ClientTableState.ResolutionState.RUNNING
                : "Can't use binding as rollback if it is in state " + rollback.getState().getResolution();
        this.rollback = rollback;
    }

    public static ActiveTableBinding create(JsTable table, ClientTableState state) {
        assert state.getActiveBinding(table) == null : "Cannot create binding for table more than once";
        final ActiveTableBinding sub = new ActiveTableBinding(table, state);
        if (!state.isRunning()) {
            state.onFailed(e -> {
                if (table.isClosed()) {
                    return;
                }
                // State failed; rollback if needed.
                if (table.getBinding() == sub) {
                    // we were the head of the table, go back and find a good node
                    sub.rollback();
                    sub.setRollback(null);
                }
            }, () -> {
                if (table.isAlive() && table.getBinding() == sub) {
                    sub.setRollback(null);
                }
            });
        }
        return sub;
    }

    @Override
    public void rollback() {
        if (state.isRunning()) {
            table.setState(state);
            return;
        }
        assert rollback != null;
        assert table.getBinding() == this : "You should only perform a rollback from the current active state";
        switch (rollback.getState().getResolution()) {
            case RUNNING:
                table.setState(rollback.getState());
                break;
            case FAILED:
                // we are a failed state. Keep rolling back.
                rollback.rollback();
                break;
            case RESOLVED:
            case UNRESOLVED:
                // our rollback is in a pending state. Update the table's expected state
                // so that our rollback will act as the master binding when it resolves.
                table.setState(rollback.getState());
                break;
            default:
                throw new UnsupportedOperationException("Cannot rollback to a " + rollback.getState() + " state");
        }
    }

    public ActiveTableBinding copyBinding(JsTable table) {
        final ActiveTableBinding sub = state.createBinding(table);
        copyRollbacks(sub);
        return sub;
    }

    public ActiveTableBinding getRollback() {
        if (rollback == null) {
            return null;
        }
        return rollback.getActiveBinding();
    }

    private void copyRollbacks(ActiveTableBinding sub) {
        if (rollback != null) {
            final ActiveTableBinding previous = rollback.getActiveBinding();
            final ClientTableState prevState = previous.getState();
            final ActiveTableBinding copy = create(sub.table, prevState);
            assert copy != null : "You must create paused states before we compute rollbacks";
            prevState.addPaused(sub.table, copy.paused);
            sub.setRollback(copy.paused);
            if (!prevState.isRunning()) {
                // recurse only if we haven't hit a known-good state yet
                rollback.getActiveBinding().copyRollbacks(copy);
            }
        }
    }

    public PausedTableBinding getPaused() {
        return paused;
    }

    public Viewport getSubscription() {
        return viewport;
    }

    public void setViewport(Viewport viewport) {
        this.viewport = viewport;
    }

    public RangeSet setDesiredViewport(long firstRow, long lastRow, Column[] columns) {
        this.rows = RangeSet.ofRange(firstRow, lastRow);
        this.columns = columns;
        subscriptionPending = true;
        return rows;
    }

    public void setDesiredSubscription(Column[] columns) {
        assert this.rows == null;
        this.columns = columns;
        subscriptionPending = true;
    }

    public RangeSet getRows() {
        return rows;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setSubscriptionPending(boolean subscriptionPending) {
        this.subscriptionPending = subscriptionPending;
    }

    public boolean isSubscriptionPending() {
        return subscriptionPending;
    }

    public void maybeReviveSubscription() {
        if (subscriptionPending || viewport != null) {
            if (rows != null) {
                state.setDesiredViewport(table, rows.getFirstRow(), rows.getLastRow(), columns);
            } else {
                state.subscribe(table, columns);
            }
        }
    }
}

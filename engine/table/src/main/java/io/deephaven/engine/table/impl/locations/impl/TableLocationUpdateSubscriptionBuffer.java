/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationState;
import org.jetbrains.annotations.NotNull;

/**
 * Intermediates between push-based subscription to a TableLocation and polling on update source refresh.
 */
public class TableLocationUpdateSubscriptionBuffer implements TableLocation.Listener {

    private final TableLocation tableLocation;

    private boolean subscribed = false;

    private final Object updateLock = new Object();
    private boolean observedNonNullSize = false;
    private boolean pendingUpdate = false;
    private TableDataException pendingException = null;

    public TableLocationUpdateSubscriptionBuffer(@NotNull final TableLocation tableLocation) {
        this.tableLocation = Require.neqNull(tableLocation, "tableLocation");
    }

    /**
     * Subscribe if needed, and return whether there was a pending update to the table location, or throw a pending
     * exception. If a pending exception is thrown, this signals that the subscription is no longer valid and no
     * subsequent pending updates will be returned.
     *
     * @return Whether there was a pending update
     */
    public synchronized boolean processPending() {
        if (!subscribed) {
            if (tableLocation.supportsSubscriptions()) {
                tableLocation.subscribe(this);
            } else {
                // NB: Locations that don't support subscriptions don't tick - this single call to run is
                // sufficient.
                tableLocation.refresh();
                handleUpdate();
            }
            subscribed = true;
        }
        final boolean resultUpdate;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultUpdate = pendingUpdate;
            pendingUpdate = false;
            resultException = pendingException;
            pendingException = null;
        }
        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
        }
        return resultUpdate;
    }

    /**
     * Unsubscribe and clear any state pending processing.
     */
    public synchronized void reset() {
        if (subscribed) {
            if (tableLocation.supportsSubscriptions()) {
                tableLocation.unsubscribe(this);
            }
            subscribed = false;
        }
        synchronized (updateLock) {
            observedNonNullSize = false;
            pendingUpdate = false;
            pendingException = null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocation.ShiftObliviousListener implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void handleUpdate() {
        synchronized (updateLock) {
            if (observedNonNullSize) {
                if (tableLocation.getSize() == TableLocationState.NULL_SIZE) {
                    pendingException = new TableDataException(
                            "Location " + tableLocation + " is no longer available, data has been removed or replaced");
                    // No need to bother unsubscribing - the consumer will either leak (and allow asynchronous cleanup)
                    // or unsubscribe all of its locations as a result of handling this exception when it polls.
                }
            } else if (tableLocation.getSize() != TableLocationState.NULL_SIZE) {
                observedNonNullSize = true;
            }
            pendingUpdate = true;
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}

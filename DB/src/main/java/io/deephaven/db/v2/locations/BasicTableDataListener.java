package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

/**
 * Common base interface for table data listeners.
 */
public interface BasicTableDataListener {

    /**
     * Notify the listener that an exception was encountered while initiating or maintaining the
     * subscription. Delivery of an exception implies that the subscription is no longer valid. This
     * might happen <i>during</i> subscription establishment, and consequently should be checked for
     * after subscribe completes.
     *
     * @param exception The exception
     */
    void handleException(@NotNull TableDataException exception);
}

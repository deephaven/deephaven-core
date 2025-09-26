//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Represents a non-viewport subscription to a table, and all data currently known to be present in the subscribed
 * columns. This class handles incoming snapshots and deltas, and fires events to consumers to notify of data changes.
 * <p>
 * Unlike {@link TableViewportSubscription}, the "original" table does not have a reference to this instance, only the
 * "private" table instance does, since the original cannot modify the subscription, and the private instance must
 * forward data to it.
 * <p>
 * Represents a subscription to the table on the server. Changes made to the table will not be reflected here - the
 * subscription must be closed and a new one optioned to see those changes. The event model is slightly different from
 * viewports to make it less expensive to compute for large tables.
 */
@JsType(namespace = "dh")
public final class TableSubscription extends AbstractTableSubscription {

    private final JsArray<Column> columns;
    private final Double updateIntervalMs;
    private final int previewListLengthLimit;

    @JsIgnore
    private TableSubscription(ClientTableState state, WorkerConnection connection,
            DataOptions.SubscriptionOptions options) {
        super(SubscriptionType.FULL_SUBSCRIPTION, state, connection);
        this.columns = options.columns;
        this.updateIntervalMs = options.updateIntervalMs;
        this.previewListLengthLimit = getPreviewListLengthLimit(options);
    }

    public static TableSubscription createTableSubscription(DataOptions.SubscriptionOptions options,
            JsTable existingTable) {
        WorkerConnection connection = existingTable.getConnection();
        ClientTableState tableState = existingTable.state();
        ClientTableState previewedState = createPreview(connection, tableState, options.previewOptions);

        return new TableSubscription(previewedState, connection, options);
    }

    @Override
    protected void sendFirstSubscriptionRequest() {
        changeSubscription(columns, updateIntervalMs);
    }

    /**
     * Updates the subscription to use the given columns and update interval.
     * 
     * @param columns the new columns to subscribe to
     * @param updateIntervalMs the new update interval, or null/omit to use the default of one second
     */
    public void changeSubscription(JsArray<Column> columns, @JsNullable Double updateIntervalMs) {
        if (updateIntervalMs != null && !updateIntervalMs.equals(this.updateIntervalMs)) {
            throw new IllegalArgumentException(
                    "Can't change refreshIntervalMs on a later call to setViewport, it must be consistent or omitted");
        }
        sendBarrageSubscriptionRequest(null, columns, updateIntervalMs, false, previewListLengthLimit);
    }

    /**
     * Update the options for this viewport subscription. This cannot alter the update interval or preview options.
     *
     * @param options the subscription options
     */
    @JsMethod
    public void update(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options) {
        DataOptions.SubscriptionOptions subscriptionOptions = DataOptions.SubscriptionOptions.of(options);
        if (subscriptionOptions.updateIntervalMs != null
                && !subscriptionOptions.updateIntervalMs.equals(this.updateIntervalMs)) {
            throw new IllegalArgumentException(
                    "Can't change refreshIntervalMs on a later call to setViewport, it must be consistent or omitted");
        }
        if (subscriptionOptions.previewOptions != null) {
            throw new IllegalArgumentException("Can't change preview options on an existing subscription");
        }
        sendBarrageSubscriptionRequest(null, subscriptionOptions.columns, updateIntervalMs, false,
                previewListLengthLimit);
    }

    @JsProperty
    @Override
    public JsArray<Column> getColumns() {
        return super.getColumns();
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
import jsinterop.annotations.JsOptional;
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
    /**
     * The connection to the server was lost. No {@code updated} events fire until it is back; the subscription is
     * re-sent automatically, and {@link #EVENT_RECONNECT} fires when that has happened, or
     * {@link #EVENT_RECONNECTFAILED} if it cannot be.
     */
    public static final String EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT;
    /**
     * The connection is back and the subscription has been re-sent to the server; data follows in {@code updated}
     * events.
     */
    public static final String EVENT_RECONNECT = JsTable.EVENT_RECONNECT;
    /**
     * The server-side table this subscription reads could not be re-established after a reconnect. The subscription is
     * closed; create a new one from a live table. {@code event.detail} describes the failure. This says nothing about
     * the connection as a whole.
     */
    public static final String EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED;
    /**
     * The server ended this subscription with an error; {@code event.detail} is the server's message. The subscription
     * is closed and no more data will come; such failures are not recoverable, create a new subscription.
     */
    public static final String EVENT_REQUEST_FAILED = JsTable.EVENT_REQUEST_FAILED;

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
        sendBarrageSubscriptionRequest(null, columns, updateIntervalMs, false, previewListLengthLimit);
    }

    /**
     * Always throws: the server does not allow a full subscription to change. Close this subscription and create a new
     * one with the columns you want.
     *
     * @param columns ignored
     * @param updateIntervalMs ignored
     * @deprecated a full subscription cannot be changed; close it and create a new one.
     */
    @Deprecated
    public void changeSubscription(JsArray<Column> columns, @JsOptional @JsNullable Double updateIntervalMs) {
        throw new IllegalStateException("A full subscription cannot be changed; close it and create a new one");
    }

    /**
     * Always throws: the server does not allow a full subscription to change. Close this subscription and create a new
     * one with the options you want.
     *
     * @param options ignored
     * @deprecated a full subscription cannot be changed; close it and create a new one.
     */
    @Deprecated
    @JsMethod
    public void update(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options) {
        throw new IllegalStateException("A full subscription cannot be changed; close it and create a new one");
    }

    @JsProperty
    @Override
    public JsArray<Column> getColumns() {
        return super.getColumns();
    }
}

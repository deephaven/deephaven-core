/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.shared.data.DeltaUpdates;
import io.deephaven.web.shared.data.TableSnapshot;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;

/**
 * Represents a non-viewport subscription to a table, and all data currently known to be present in the subscribed
 * columns. This class handles incoming snapshots and deltas, and fires events to consumers to notify of data changes.
 *
 * Unlike {@link TableViewportSubscription}, the "original" table does not have a reference to this instance, only the
 * "private" table instance does, since the original cannot modify the subscription, and the private instance must
 * forward data to it.
 *
 * Represents a subscription to the table on the server. Changes made to the table will not be reflected here - the
 * subscription must be closed and a new one optioned to see those changes. The event model is slightly different from
 * viewports to make it less expensive to compute for large tables.
 */
@JsType(namespace = "dh")
public class TableSubscription extends HasEventHandling {

    /**
     * Indicates that some new data is available on the client, either an initial snapshot or a delta update. The
     * <b>detail</b> field of the event will contain a TableSubscriptionEventData detailing what has changed, or
     * allowing access to the entire range of items currently in the subscribed columns.
     */
    public static final String EVENT_UPDATED = "updated";


    // column defs in this subscription
    private JsArray<Column> columns;
    // holder for data
    private SubscriptionTableData data;

    // table created for this subscription
    private Promise<JsTable> copy;

    // copy from the initially given table so we don't need to way
    @JsIgnore
    public TableSubscription(JsArray<Column> columns, JsTable existingTable, Double updateIntervalMs) {

        copy = existingTable.copy(false).then(table -> new Promise<>((resolve, reject) -> {
            table.state().onRunning(newState -> {
                // TODO handle updateInterval core#188
                table.internalSubscribe(columns, this);

                resolve.onInvoke(table);
            }, table::close);
        }));

        this.columns = columns;
        Integer rowStyleColumn = existingTable.state().getRowFormatColumn() == null ? NO_ROW_FORMAT_COLUMN
                : existingTable.state().getRowFormatColumn().getIndex();
        this.data = new SubscriptionTableData(columns, rowStyleColumn, this);

    }

    // public void changeSubscription(JsArray<Column> columns) {
    // copy.then(t ->{
    // t.internalSubscribe(columns, this);
    // return Promise.resolve(t);
    // });
    // this.columns = columns;
    // }


    @JsIgnore
    public void handleSnapshot(TableSnapshot snapshot) {
        data.handleSnapshot(snapshot);
    }

    @JsIgnore
    public void handleDelta(DeltaUpdates delta) {
        data.handleDelta(delta);
    }

    /**
     * The columns that were subscribed to when this subscription was created
     * 
     * @return {@link Column}
     */
    @JsProperty
    public JsArray<Column> getColumns() {
        return columns;
    }

    /**
     * Stops the subscription on the server.
     */
    public void close() {
        copy.then(table -> {
            table.close();
            return Promise.resolve(table);
        });
    }
}

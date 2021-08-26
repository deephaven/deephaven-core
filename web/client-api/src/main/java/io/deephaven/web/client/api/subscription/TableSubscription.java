package io.deephaven.web.client.api.subscription;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.shared.data.DeltaUpdates;
import io.deephaven.web.shared.data.TableSnapshot;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

/**
 * Represents a non-viewport subscription to a table, and all data currently known to be present in the subscribed
 * columns. This class handles incoming snapshots and deltas, and fires events to consumers to notify of data changes.
 *
 * Unlike {@link TableViewportSubscription}, the "original" table does not have a reference to this instance, only the
 * "private" table instance does, since the original cannot modify the subscription, and the private instance must
 * forward data to it.
 */
public class TableSubscription extends HasEventHandling {

    @JsProperty(namespace = "dh.TableSubscription")
    public static final String EVENT_UPDATED = "updated";


    // column defs in this subscription
    private JsArray<Column> columns;
    // holder for data
    private SubscriptionTableData data;

    // table created for this subscription
    private Promise<JsTable> copy;

    // copy from the initially given table so we don't need to way
    public TableSubscription(JsArray<Column> columns, JsTable existingTable, Double updateIntervalMs) {

        copy = existingTable.copy(false).then(table -> new Promise<>((resolve, reject) -> {
            table.state().onRunning(newState -> {
                // TODO handle updateInterval core#188
                table.internalSubscribe(columns, this);

                resolve.onInvoke(table);
            }, table::close);
        }));

        this.columns = columns;
        Integer rowStyleColumn = existingTable.state().getRowFormatColumn() == null ? null
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


    public void handleSnapshot(TableSnapshot snapshot) {
        data.handleSnapshot(snapshot);
    }

    public void handleDelta(DeltaUpdates delta) {
        data.handleDelta(delta);
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return columns;
    }

    @JsMethod
    public void close() {
        copy.then(table -> {
            table.close();
            return Promise.resolve(table);
        });
    }
}

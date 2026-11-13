//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsIgnore;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * A server-side table that the client has retained: its data and metadata can be read, and it must be closed when no
 * longer needed. A {@link JsTable} is one, with its own client-side filter/sort state on top; a {@link JsResolvedTable}
 * is one that additionally offers the chainable {@link JsTableOperations}; a {@link JsTotalsTable} is one too.
 * <p>
 * Only visible directly in Java, not TypeScript - exported members are copied to subclasses instead as needed.
 */
@TsIgnore
public interface JsITable {
    /**
     * The total count of rows in the table. If there is a viewport subscription active, this size will be updated when
     * the subscription updates. If not, and the table is uncoalesced, the size will be
     * {@code dh.Table.SIZE_UNCOALESCED}. Otherwise, the size will be updated when the server's update graph processes
     * changes.
     *
     * @return the size of the table
     */
    @JsProperty
    double getSize();

    /**
     * The columns that are present on this table.
     *
     * @return the columns present in this table
     */
    @JsProperty
    ReadonlyArray<Column> getColumns();

    /**
     * Finds a column by name.
     *
     * @param columnName the name of the column
     * @return the column
     */
    @JsMethod
    Column findColumn(String columnName);

    /**
     * The names of all attributes defined on this table.
     *
     * @return an array of attributes defined on this table
     */
    @JsMethod
    ReadonlyArray<String> getAttributes();

    /**
     * Reads an attribute's value as a string. Only primitive and string values are sent to the client; an attribute
     * listed by {@link #getAttributes()} with any other value cannot be read here.
     *
     * @param attributeName the name of the attribute to read
     * @return the value of the attribute or null if none with that name exists
     */
    @JsMethod
    @JsNullable
    Object getAttribute(String attributeName);

    /**
     * Returns a promise that will resolve to a {@link TableData} instance containing a snapshot of the current state of
     * the table, within the bounds of the specified rows and columns.
     *
     * @param options options for the snapshot; see {@link DataOptions.SnapshotOptions} for details
     * @return Promise of {@link TableData}
     */
    @JsMethod
    Promise<TableData> createSnapshot(@TsTypeRef(DataOptions.SnapshotOptions.class) Object options);

    /**
     * Creates a viewport subscription to the specified columns, across the specified rows in the table. The returned
     * {@link TableViewportSubscription} instance allows the viewport to be changed over time, and events are fired from
     * it when the data changes or when a viewport change has been applied. Each call creates a new subscription, which
     * must have {@link TableViewportSubscription#close()} called on it to stop it and release its resources.
     *
     * @param options options for the viewport subscription; see {@link DataOptions.ViewportSubscriptionOptions} for
     *        details
     * @return a new {@link TableViewportSubscription}
     */
    @JsMethod
    TableViewportSubscription createViewportSubscription(
            @TsTypeRef(DataOptions.ViewportSubscriptionOptions.class) Object options);

    /**
     * Creates a subscription to the specified columns, across all rows in the table. Useful for charts or taking a
     * snapshot of the table atomically. The initial snapshot will arrive in a single event, but later changes will be
     * sent as updates. However, this may still be very expensive to run from a browser for very large tables. Each call
     * creates a new subscription, which must have {@link TableSubscription#close()} called on it to stop it and release
     * its resources, and all events are fired from the {@link TableSubscription} instance.
     *
     * @param options options for the subscription; see {@link DataOptions.SubscriptionOptions} for details
     * @return a new {@link TableSubscription}
     */
    @JsMethod
    TableSubscription createSubscription(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options);

    /**
     * Signals that the table will no longer be used and server resources should be cleaned up. Logs a warning if called
     * more than once.
     */
    @JsMethod
    void close();

    /**
     * Computes statistics for one column of this table, such as its count, sum and average for a numeric column, or its
     * most common values for other types.
     *
     * @param column the column, or the name of the column, to compute statistics for
     * @return a promise of the {@link JsColumnStatistics ColumnStatistics} for that column
     */
    @JsMethod
    Promise<JsColumnStatistics> getColumnStatistics(Column.ColumnOrName column);
}

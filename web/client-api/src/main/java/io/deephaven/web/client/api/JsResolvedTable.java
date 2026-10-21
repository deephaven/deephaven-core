//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

/**
 * A server-side table that the client has retained: its data and metadata can be read, and it must be closed when no
 * longer needed. This is what every table-producing API hands back, whatever its flavor. A {@link JsTable} is one, with
 * its own client-side filter/sort state on top; an {@link JsImmutableTable} is one that additionally offers the
 * chainable {@link JsTableOperations}. Code that only reads a table can accept this type and take either.
 */
@TsName(namespace = "dh", name = "ResolvedTable")
@TsInterface
public interface JsResolvedTable {
    /**
     * The total count of rows in the table. If there is a viewport subscription active, this size will be updated when
     * the subscription updates. If not, and the table is uncoalesced, the size will be
     * {@link JsTable#SIZE_UNCOALESCED}. Otherwise, the size will be updated when the server's update graph processes
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
     * {@code null} if no property exists, a string if it is an easily serializable property, or a promise of a table
     * that will either resolve with a table or error out if the object can't be passed to JS.
     *
     * @param attributeName the name of the attribute to read
     * @return the value of the attribute or null if none with that name exists
     */
    @JsMethod
    Object getAttribute(String attributeName);

    @JsMethod
    Promise<TableData> createSnapshot(@TsTypeRef(DataOptions.SnapshotOptions.class) Object options);

    @JsMethod
    TableViewportSubscription createViewportSubscription(
            @TsTypeRef(DataOptions.ViewportSubscriptionOptions.class) Object options);

    @JsMethod
    TableSubscription createSubscription(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options);

    /**
     * Signals that the table will no longer be used and server resources should be cleaned up. Logs a warning if called
     * more than once.
     */
    // TODO support Symbol.dispose, if present
    @JsMethod
    void close();

    // /**
    // * Seek the row matching the data provided
    // *
    // * @param startingRow Row to start the seek from
    // * @param column Column to seek for value on
    // * @param valueType Type of value provided
    // * @param seekValue Value to seek
    // * @param insensitive Optional value to flag a search as case-insensitive. Defaults to {@code false}.
    // * @param contains Optional value to have the seek value do a contains search instead of exact equality. Defaults
    // to
    // * {@code false}.
    // * @param isBackwards Optional value to seek backwards through the table instead of forwards. Defaults to
    // * {@code false}.
    // * @return A promise that resolves to the row position of the value found.
    // */
    // //TODO options param
    // @JsMethod
    // Promise<Double> seekRow(
    // TableData.RowPositionUnion startingRow,
    // Column column,
    // @TsTypeRef(ValueType.class) String valueType,
    // Any seekValue,
    // @JsOptional @JsNullable Boolean insensitive,
    // @JsOptional @JsNullable Boolean contains,
    // @JsOptional @JsNullable Boolean isBackwards);

    // /**
    // * A promise that will resolve to {@link JsColumnStatistics ColumnStatistics} for the column of this table.
    // *
    // * @param column the column to get statistics for
    // * @return Promise of {@link JsColumnStatistics ColumnStatistics}
    // */
    // //TODO change return to a pending table, expose col stats ctor to help?
    // @JsMethod
    // Promise<JsColumnStatistics> getColumnStatistics(Column.ColumnOrName column);
}

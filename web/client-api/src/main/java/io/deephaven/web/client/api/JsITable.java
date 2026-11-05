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
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * Strawman name, common supertype for all table-like things. Exposed to JS as part of PendingTable's own generics and
 * the resolve() method.
 * <p>
 * A server-side table that the client has retained: its data and metadata can be read, and it must be closed when no
 * longer needed. This is what every table-producing API hands back, whatever its flavor. A {@link JsTable} is one, with
 * its own client-side filter/sort state on top; an {@link JsResolvedTable} is one that additionally offers the
 * chainable {@link JsTableOperations}. Code that only reads a table can accept this type and take either.
 */
// TODO once the doclet honors @TsTypeRef on a type-parameter bound: make this @TsIgnore (the doclet already flattens an
// ignored supertype's members into its subtypes), add a nested Union of Table | TotalsTable | ResolvedTable, and point
// TableOperations.resolve() and the PendingTable<T> bound at that union via @TsTypeRef. Java keeps this interface for
// its own typing; TS should never see the name.
@TsName(namespace = "dh", name = "ITable")
@TsInterface
public interface JsITable {
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
     * Reads an attribute's value as a string. Only primitive and string values are sent to the client; an attribute
     * listed by {@link #getAttributes()} with any other value cannot be read here.
     *
     * @param attributeName the name of the attribute to read
     * @return the value of the attribute or null if none with that name exists
     */
    @JsMethod
    @JsNullable
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

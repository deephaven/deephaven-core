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
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

/**
 * A server-side table that the client has retained: its data and metadata can be read, and it must be closed when no
 * longer needed. A {@link JsTable} is one, with its own client-side filter/sort state on top; a {@link JsResolvedTable}
 * is one that additionally offers the chainable {@link JsTableOperations}; a {@link JsTotalsTable} is one too.
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
    @JsMethod
    void close();

    /**
     * Finds the next row, after {@code startingRow} and wrapping around, whose value in {@code column} matches
     * {@code seekValue}.
     *
     * @param startingRow the position of the row to search after
     * @param column the column, or the name of the column, to search
     * @param valueType the type to compare {@code seekValue} as
     * @param seekValue the value to find
     * @param insensitive optionally, {@code true} to ignore case. Defaults to {@code false}.
     * @param contains optionally, {@code true} to match values that contain {@code seekValue} rather than equal it.
     *        Defaults to {@code false}.
     * @param isBackwards optionally, {@code true} to search backward. Defaults to {@code false}.
     * @return a promise of the position of the matching row, or -1 if no row matches
     */
    // TODO BEFORE MERGE: ask reviewers whether to also accept an options object here, as a union with startingRow for
    // the first argument ({column, value, valueType?, startingRow?, caseInsensitive?, contains?, backward?}). It keeps
    // Table.seekRow's positional signature, which is what this matches today.
    @JsMethod
    Promise<Double> seekRow(
            double startingRow,
            Column.ColumnOrName column,
            @TsTypeRef(ValueType.class) String valueType,
            Any seekValue,
            @JsOptional @JsNullable Boolean insensitive,
            @JsOptional @JsNullable Boolean contains,
            @JsOptional @JsNullable Boolean isBackwards);

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

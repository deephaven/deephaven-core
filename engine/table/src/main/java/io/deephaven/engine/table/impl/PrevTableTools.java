//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;

/**
 * Tools for presenting the previous state of a table, as {@link PrevColumnSource} presents the previous values of a
 * column. A view of the previous state is good for one update cycle only.
 */
@InternalUseOnly
public final class PrevTableTools {

    private PrevTableTools() {}

    /**
     * Make a view of the previous state of {@code table}, good only during the updating phase of the current update
     * cycle: the rows of the previous row set, and the previous value of each column.
     * <p>
     * The view drifts once the cycle ends. The view copies the previous row set once, when made, but the columns read
     * previous values on every access, and those values become the current values when the cycle ends. Previous values
     * are defined only during the updating phase, so make the view only during that phase, use the view only within the
     * operation that makes the view, and never publish the view. A snapshot that uses previous values meets these
     * conditions, since the snapshot discards any attempt that a cycle boundary interrupts.
     * <p>
     * A column of {@link io.deephaven.engine.rowset.TrackingRowSet tracking row sets}, such as the row set column of a
     * data index table, presents previous row sets only if the column's previous values are the row sets'
     * {@link io.deephaven.engine.rowset.TrackingRowSet#prev() previous} row sets, as a
     * {@link io.deephaven.engine.table.impl.sources.RowSetColumnSourceWrapper} column's are. Those previous row sets
     * are valid only during the updating phase as well.
     * <p>
     * The view carries the attributes of {@code table} that a {@link Table#coalesce() coalesce} carries: the attributes
     * that describe the data and hold in every state, such as {@link Table#SORTED_COLUMNS_ATTRIBUTE sortedness}, but
     * not the attributes tied to the live table, such as {@link Table#INPUT_TABLE_ATTRIBUTE} and
     * {@link Table#MERGED_TABLE_ATTRIBUTE}. The view is {@link Table#isFlat() flat} if {@code table} is flat.
     *
     * @param table The table
     * @return {@code table} itself if {@code table} is static; otherwise a static table over the previous state of
     *         {@code table}, good only during the updating phase of the current update cycle
     */
    public static Table prevTable(@NotNull final Table table) {
        if (!table.isRefreshing()) {
            return table;
        }
        final LinkedHashMap<String, ColumnSource<?>> columns = new LinkedHashMap<>(table.numColumns());
        table.getColumnSourceMap().forEach((name, source) -> columns.put(name,
                source.isImmutable() ? source : source.getPrevSource()));
        final QueryTable result = new QueryTable(table.getDefinition(), table.getRowSet().copyPrev().toTracking(),
                columns, null, table.getAttributes(
                        name -> BaseTable.shouldCopyAttribute(name, BaseTable.CopyAttributeOperation.Coalesce)));
        if (table.isFlat()) {
            // A flat table stays flat after every update, so the previous row set is flat as well.
            result.setFlat();
        }
        return result;
    }
}

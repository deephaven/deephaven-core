/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * Transformer to adjust aggregation results for operation building.
 */
public interface AggregationContextTransformer {

    /**
     * After we have created the key columns, and the default result columns, allow each transformer to add additional
     * columns to the result set that are not handled by the regular modified column set transformer, etc. logic.
     */
    default void resultColumnFixup(@NotNull final Map<String, ColumnSource<?>> resultColumns) {}

    /**
     * Before we return the result, each transformer has a chance to replace it or change it as it sees fit. Practically
     * this is used to change the attributes for rollups and trees.
     */
    default QueryTable transformResult(@NotNull final QueryTable table) {
        return table;
    }

    /**
     * The helper calls the transformer with a supplier to create a suitable {@link AggregationRowLookup row lookup}
     * function for the result table.
     *
     * @param rowLookupFactory Factory for a function that translates an opaque key to an integer row position in the
     *        result table, which is also the row key in the result table
     * 
     */
    default void supplyRowLookup(@NotNull final Supplier<AggregationRowLookup> rowLookupFactory) {}
}

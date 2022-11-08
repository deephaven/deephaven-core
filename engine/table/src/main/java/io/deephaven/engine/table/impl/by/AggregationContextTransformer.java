/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;

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
    default void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {}

    /**
     * Before we return the result, each transformer has a chance to replace it or change it as it sees fit. Practically
     * this is used to change the attributes for rollups and trees..
     */
    default QueryTable transformResult(QueryTable table) {
        return table;
    }

    /**
     * The empty key, for use in (trivial) reverse lookups against no-key aggregations.
     */
    Object EMPTY_KEY = new Object();

    /**
     * The unknown row, returned by reverse lookup functions when the supplied key is not found.
     */
    int UNKNOWN_ROW = (int) RowSequence.NULL_ROW_KEY;

    /**
     * The helper calls the transformer with a supplier to create a suitable reverse lookup function for this table.
     *
     * @param reverseLookupFactory Factory for a function that translates an opaque key to an integer position in the
     *        output, which is also the row key in the result table. Missing keys will map to {@value #UNKNOWN_ROW}.
     * @apiNote "Empty" keys are signified by the {@link #EMPTY_KEY} object. Singular keys are (boxed, if needed)
     *          objects. Compound keys are arrays of (boxed, if needed) objects, in the order of the aggregations
     *          group-by columns. Reinterpretation, if needed, will be applied internally to the reverse lookup
     *          function.
     */
    default void supplyReverseLookup(Supplier<ToIntFunction<Object>> reverseLookupFactory) {}
}

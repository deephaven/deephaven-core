//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

public interface GroupByOperator extends IterativeChunkedAggregationOperator {
    /**
     * Get a map from input column names to the corresponding output {@link ColumnSource}.
     */
    Map<String, ? extends ColumnSource<?>> getInputResultColumns();

    /**
     * Given that there have been modified input columns, should we propagate changes?
     * 
     * @param columnsModified have any of the input columns been modified (as per the MCS)?
     * @return true if we have modified our output (e.g., because of additions or modifications)
     */
    boolean hasModifications(final boolean columnsModified);
}

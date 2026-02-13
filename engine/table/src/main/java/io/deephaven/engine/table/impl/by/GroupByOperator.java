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
     * Determine whether to propagate changes when input columns have been modified.
     *
     * @param columnsModified have any of the input columns been modified (as per the MCS)?
     * @return true if we have modified our output (e.g., because of additions or modifications).
     */
    boolean hasModifications(final boolean columnsModified);
}

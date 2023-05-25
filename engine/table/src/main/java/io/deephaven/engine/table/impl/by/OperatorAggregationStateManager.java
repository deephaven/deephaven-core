/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

interface OperatorAggregationStateManager {

    int maxTableSize();

    SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize);

    void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources, MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions);

    ColumnSource[] getKeyHashTableSources();

    int UNKNOWN_ROW = AggregationRowLookup.DEFAULT_UNKNOWN_ROW;

    /**
     * Implement a lookup in order to support {@link AggregationRowLookup#get(Object)}.
     * 
     * @param key The opaque group-by key to find the row position/key for
     * @return The row position/key for {@code key} in the result table, or {@value #UNKNOWN_ROW} if not found
     */
    int findPositionForKey(Object key);
}

package io.deephaven.engine.v2.by;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

interface ChunkedOperatorAggregationStateManager {

    SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize);

    void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources, MutableInt nextOutputPosition, WritableIntChunk<Attributes.RowKeys> outputPositions);

    ColumnSource[] getKeyHashTableSources();

    int findPositionForKey(Object key);
}

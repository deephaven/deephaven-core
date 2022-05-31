/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util.freezeby;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

class DoubleFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final DoubleArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    DoubleFreezeByHelper(WritableColumnSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (DoubleArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Values> values, IntChunk<ChunkPositions> startPositions, IntChunk<RowKeys> destinations, IntChunk<ChunkLengths> length) {
        final DoubleChunk asDouble = values.asDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            // if there is just row churn for a key, we don't want to re-snapshot the value (adds/removes just get ignored)
            if (rowCount.wasDestinationEmpty(destination)) {
                resultSource.set(destination, asDouble.get(position));
            }
        }
    }


    @Override
    public void addChunk(Chunk<? extends Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final DoubleChunk asDouble = values.asDoubleChunk();
            resultSource.set(destination, asDouble.get(0));
        }
    }

    @Override
    public void clearIndex(RowSequence removed) {
        // region clearIndex
        // endregion clearIndex
    }
}

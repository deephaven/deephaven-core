/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util.freezeby;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.AbstractLongArraySource;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.rowset.RowSequence;

class LongFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final AbstractLongArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    LongFreezeByHelper(WritableColumnSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (AbstractLongArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.RowKeys> destinations, IntChunk<Attributes.ChunkLengths> length) {
        final LongChunk asLong = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            // if there is just row churn for a key, we don't want to re-snapshot the value (adds/removes just get ignored)
            if (rowCount.wasDestinationEmpty(destination)) {
                resultSource.set(destination, asLong.get(position));
            }
        }
    }


    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final LongChunk asLong = values.asLongChunk();
            resultSource.set(destination, asLong.get(0));
        }
    }

    @Override
    public void clearIndex(RowSequence removed) {
        // region clearIndex
        // endregion clearIndex
    }
}

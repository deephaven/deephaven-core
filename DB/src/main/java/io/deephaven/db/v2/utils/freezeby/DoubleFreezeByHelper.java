/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils.freezeby;

import io.deephaven.db.v2.sources.DoubleArraySource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;

class DoubleFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final DoubleArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    DoubleFreezeByHelper(WritableSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (DoubleArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.KeyIndices> destinations, IntChunk<Attributes.ChunkLengths> length) {
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
    public void addChunk(Chunk<? extends Attributes.Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final DoubleChunk asDouble = values.asDoubleChunk();
            resultSource.set(destination, asDouble.get(0));
        }
    }

    @Override
    public void clearIndex(OrderedKeys removed) {
        // region clearIndex
        // endregion clearIndex
    }
}

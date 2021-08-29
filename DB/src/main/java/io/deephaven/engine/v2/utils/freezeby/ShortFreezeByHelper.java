/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils.freezeby;

import io.deephaven.engine.v2.sources.ShortArraySource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.v2.utils.OrderedKeys;

class ShortFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final ShortArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    ShortFreezeByHelper(WritableSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (ShortArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.KeyIndices> destinations, IntChunk<Attributes.ChunkLengths> length) {
        final ShortChunk asShort = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            // if there is just row churn for a key, we don't want to re-snapshot the value (adds/removes just get ignored)
            if (rowCount.wasDestinationEmpty(destination)) {
                resultSource.set(destination, asShort.get(position));
            }
        }
    }


    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final ShortChunk asShort = values.asShortChunk();
            resultSource.set(destination, asShort.get(0));
        }
    }

    @Override
    public void clearIndex(OrderedKeys removed) {
        // region clearIndex
        // endregion clearIndex
    }
}

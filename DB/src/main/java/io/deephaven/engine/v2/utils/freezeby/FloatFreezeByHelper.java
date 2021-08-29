/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils.freezeby;

import io.deephaven.engine.v2.sources.FloatArraySource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.v2.utils.OrderedKeys;

class FloatFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final FloatArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    FloatFreezeByHelper(WritableSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (FloatArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.KeyIndices> destinations, IntChunk<Attributes.ChunkLengths> length) {
        final FloatChunk asFloat = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            // if there is just row churn for a key, we don't want to re-snapshot the value (adds/removes just get ignored)
            if (rowCount.wasDestinationEmpty(destination)) {
                resultSource.set(destination, asFloat.get(position));
            }
        }
    }


    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final FloatChunk asFloat = values.asFloatChunk();
            resultSource.set(destination, asFloat.get(0));
        }
    }

    @Override
    public void clearIndex(OrderedKeys removed) {
        // region clearIndex
        // endregion clearIndex
    }
}

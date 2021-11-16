/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFreezeByHelper and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils.freezeby;

import io.deephaven.engine.v2.sources.ObjectArraySource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.rowset.RowSequence;

class ObjectFreezeByHelper implements FreezeByOperator.FreezeByHelper {
    private final ObjectArraySource resultSource;
    private final FreezeByCountOperator rowCount;

    ObjectFreezeByHelper(WritableColumnSource resultSource, FreezeByCountOperator rowCount) {
        this.resultSource = (ObjectArraySource)resultSource;
        this.rowCount = rowCount;
    }

    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.RowKeys> destinations, IntChunk<Attributes.ChunkLengths> length) {
        final ObjectChunk asObject = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            // if there is just row churn for a key, we don't want to re-snapshot the value (adds/removes just get ignored)
            if (rowCount.wasDestinationEmpty(destination)) {
                resultSource.set(destination, asObject.get(position));
            }
        }
    }


    @Override
    public void addChunk(Chunk<? extends Attributes.Values> values, long destination) {
        if (rowCount.wasDestinationEmpty(destination)) {
            final ObjectChunk asObject = values.asObjectChunk();
            resultSource.set(destination, asObject.get(0));
        }
    }

    @Override
    public void clearIndex(RowSequence removed) {
        // region clearIndex
        removed.forAllRowKeys(idx -> resultSource.set(idx, null));
        // endregion clearIndex
    }
}

package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.util.SafeCloseable;

public class ReAvgVarOrderingContext implements SafeCloseable {
    public final WritableLongChunk<RowKeys> keyIndices;
    final int size;
    WritableIntChunk<ChunkPositions> statePositions;
    LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext;
    boolean ordered;

    public ReAvgVarOrderingContext(int size) {
        this.size = size;
        keyIndices = WritableLongChunk.makeWritableChunk(size);
    }


    public void ensureUnordered() {
        statePositions = WritableIntChunk.makeWritableChunk(size);
        sortKernelContext = LongIntTimsortKernel.createContext(size);
    }

    @Override
    public void close() {
        keyIndices.close();
        if (sortKernelContext != null) {
            statePositions.close();
            sortKernelContext.close();
        }
    }

    RowSequence destinationSequenceFromChunks(IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions) {
        keyIndices.setSize(startPositions.size());
        long lastDestination = -1L;
        ordered = true;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int destination = destinations.get(startPosition);
            if (destination < lastDestination) {
                ordered = false;
            }
            keyIndices.set(ii, destination);
            lastDestination = destination;
        }
        if (!ordered) {
            ensureUnordered();
            statePositions.setSize(keyIndices.size());
            ChunkUtils.fillInOrder(statePositions);
            LongIntTimsortKernel.sort(sortKernelContext, statePositions, keyIndices);
        }
        return RowSequenceFactory.wrapRowKeysChunkAsRowSequence(LongChunk.downcast(keyIndices));
    }
}

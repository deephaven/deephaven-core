//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.mutable.MutableInt;

public class CharBarrageRunKernel {

    static final BarrageRunKernel INSTANCE = new Impl();

    private static class Impl implements BarrageRunKernel {
        @Override
        public void encodeRunEnds(
                final Chunk<Values> src,
                final RowSet subset,
                final WritableIntChunk<Values> runEnds,
                final WritableChunk<Values> runValues) {
            final CharChunk<Values> typedSrc = src.asCharChunk();
            final WritableCharChunk<Values> typedRunValues = runValues.asWritableCharChunk();
            typedRunValues.setSize(0);
            runEnds.setSize(0);

            // subset will always contain at least BarrageUtil#REE_MIN_SAMPLE_SIZE values
            final long firstKey = subset.firstRowKey();
            // Acceptable use of arrays to prevent boxing/unboxing from Mutable<T>
            final char[] prev = {typedSrc.get((int) firstKey)};
            final MutableInt logicalPos = new MutableInt(1);

            subset.forAllRowKeys(key -> {
                if (key == firstKey) {
                    return;
                }
                final char cur = typedSrc.get((int) key);
                if (!CharComparisons.eq(prev[0], cur)) {
                    runEnds.add(logicalPos.get());
                    typedRunValues.add(prev[0]);
                    prev[0] = cur;
                }
                logicalPos.increment();
            });

            // Final run
            runEnds.add(logicalPos.get());
            typedRunValues.add(prev[0]);
        }

        @Override
        public void decodeRunEnds(
                final IntChunk<Values> runEnds,
                final Chunk<Values> runValues,
                final WritableChunk<Values> dst,
                final int outOffset) {
            final CharChunk<Values> typedRunValues = runValues.asCharChunk();
            final WritableCharChunk<Values> typedDst = dst.asWritableCharChunk();
            int start = 0;
            final int numRuns = runValues.size();
            for (int runIndex = 0; runIndex < numRuns; ++runIndex) {
                final int end = runEnds.get(runIndex);
                typedDst.fillWithValue(outOffset + start, end - start, typedRunValues.get(runIndex));
                start = end;
            }
        }
    }
}

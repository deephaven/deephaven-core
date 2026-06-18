//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageRunKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.LongComparisons;

public class LongBarrageRunKernel {

    static final BarrageRunKernel INSTANCE = new Impl();

    private static class Impl implements BarrageRunKernel {
        @Override
        public void encodeRunEnds(
                final Chunk<Values> src,
                final RowSequence subset,
                final WritableIntChunk<Values> runEnds,
                final WritableChunk<Values> runValues) {
            final LongChunk<Values> typedSrc = src.asLongChunk();
            final WritableLongChunk<Values> typedRunValues = runValues.asWritableLongChunk();
            typedRunValues.setSize(0);
            runEnds.setSize(0);

            try (final RowSequence.Iterator rsIt = subset.getRowSequenceIterator()) {
                // subset will always contain at least BarrageUtil#REE_MIN_SAMPLE_SIZE values
                long key = rsIt.peekNextKey();
                long prev = typedSrc.get((int) key);
                rsIt.advance(key + 1);
                int logicalPos = 1;

                while (rsIt.hasMore()) {
                    key = rsIt.peekNextKey();
                    final long cur = typedSrc.get((int) key);
                    rsIt.advance(key + 1);
                    if (!LongComparisons.eq(prev, cur)) {
                        runEnds.add(logicalPos);
                        typedRunValues.add(prev);
                        prev = cur;
                    }
                    logicalPos++;
                }

                // Final run
                runEnds.add(logicalPos);
                typedRunValues.add(prev);
            }
        }

        @Override
        public void decodeRunEnds(
                final IntChunk<Values> runEnds,
                final Chunk<Values> runValues,
                final WritableChunk<Values> dst,
                final int outOffset) {
            final LongChunk<Values> typedRunValues = runValues.asLongChunk();
            final WritableLongChunk<Values> typedDst = dst.asWritableLongChunk();
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

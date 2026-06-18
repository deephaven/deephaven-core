//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageRunKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.ByteComparisons;

public class ByteBarrageRunKernel {

    static final BarrageRunKernel INSTANCE = new Impl();

    private static class Impl implements BarrageRunKernel {
        @Override
        public void encodeRunEnds(
                final Chunk<Values> src,
                final RowSequence subset,
                final WritableIntChunk<Values> runEnds,
                final WritableChunk<Values> runValues) {
            final ByteChunk<Values> typedSrc = src.asByteChunk();
            final WritableByteChunk<Values> typedRunValues = runValues.asWritableByteChunk();
            typedRunValues.setSize(0);
            runEnds.setSize(0);

            try (final RowSequence.Iterator rsIt = subset.getRowSequenceIterator()) {
                // subset will always contain at least BarrageUtil#REE_MIN_SAMPLE_SIZE values
                long key = rsIt.peekNextKey();
                byte prev = typedSrc.get((int) key);
                rsIt.advance(key + 1);
                int logicalPos = 1;

                while (rsIt.hasMore()) {
                    key = rsIt.peekNextKey();
                    final byte cur = typedSrc.get((int) key);
                    rsIt.advance(key + 1);
                    if (!ByteComparisons.eq(prev, cur)) {
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
            final ByteChunk<Values> typedRunValues = runValues.asByteChunk();
            final WritableByteChunk<Values> typedDst = dst.asWritableByteChunk();
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

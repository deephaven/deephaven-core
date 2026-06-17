//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharBarrageRunKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.ShortComparisons;
import java.util.function.IntConsumer;

public class ShortBarrageRunKernel {

    static final BarrageRunKernel INSTANCE = new Impl();

    private static class Impl implements BarrageRunKernel {
        @Override
        public void computeRuns(
                final Chunk<Values> src,
                final RowSequence subset,
                final WritableChunk<Values> runEnds,
                final WritableChunk<Values> runValues) {
            final ShortChunk<Values> typedSrc = src.asShortChunk();
            final WritableShortChunk<Values> typedRunValues = runValues.asWritableShortChunk();
            final IntConsumer adder = BarrageRunKernel.runEndAdder(runEnds);
            typedRunValues.setSize(0);
            runEnds.setSize(0);

            try (final RowSequence.Iterator rsIt = subset.getRowSequenceIterator()) {
                // subset will always contain at least BarrageUtil#REE_MIN_SAMPLE_SIZE values
                long key = rsIt.peekNextKey();
                short prev = typedSrc.get((int) key);
                rsIt.advance(key + 1);
                int logicalPos = 1;

                while (rsIt.hasMore()) {
                    key = rsIt.peekNextKey();
                    final short cur = typedSrc.get((int) key);
                    rsIt.advance(key + 1);
                    if (!ShortComparisons.eq(prev, cur)) {
                        adder.accept(logicalPos);
                        typedRunValues.add(prev);
                        prev = cur;
                    }
                    logicalPos++;
                }

                // Final run
                adder.accept(logicalPos);
                typedRunValues.add(prev);
            }
        }
    }
}

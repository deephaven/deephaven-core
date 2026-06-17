//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.compare.CharComparisons;
import java.util.function.IntConsumer;

public class CharBarrageRunKernel {

    static final BarrageRunKernel INSTANCE = new Impl();

    private static class Impl implements BarrageRunKernel {
        @Override
        public void computeRuns(
                final Chunk<Values> src,
                final RowSequence subset,
                final WritableChunk<Values> runEnds,
                final WritableChunk<Values> runValues) {
            final CharChunk<Values> typedSrc = src.asCharChunk();
            final WritableCharChunk<Values> typedRunValues = runValues.asWritableCharChunk();
            final IntConsumer adder = BarrageRunKernel.runEndAdder(runEnds);
            typedRunValues.setSize(0);
            runEnds.setSize(0);

            try (final RowSequence.Iterator rsIt = subset.getRowSequenceIterator()) {
                // subset will always contain at least BarrageUtil#REE_MIN_SAMPLE_SIZE values
                long key = rsIt.peekNextKey();
                char prev = typedSrc.get((int) key);
                rsIt.advance(key + 1);
                int logicalPos = 1;

                while (rsIt.hasMore()) {
                    key = rsIt.peekNextKey();
                    final char cur = typedSrc.get((int) key);
                    rsIt.advance(key + 1);
                    if (!CharComparisons.eq(prev, cur)) {
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

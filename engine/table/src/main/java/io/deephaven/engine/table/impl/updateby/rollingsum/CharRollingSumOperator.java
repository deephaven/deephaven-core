//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class CharRollingSumOperator extends BaseLongUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected CharChunk<? extends Values> charInfluencerValuesChunk;
        protected CharRingBuffer charWindowValues;

        protected Context(final int chunkSize) {
            super(chunkSize);
            charWindowValues = new CharRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            charWindowValues = null;
        }


        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            charInfluencerValuesChunk = valueChunks[0].asCharChunk();
        }

        @Override
        public void push(int pos, int count) {
            charWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                char val = charInfluencerValuesChunk.get(pos + ii);
                charWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_CHAR) {
                    if (curVal == NULL_LONG) {
                        curVal = val;
                    } else {
                        curVal += val;
                    }
                } else {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(charWindowValues.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                char val = charWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_CHAR) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (charWindowValues.size() == nullCount) {
                outputValues.set(outIdx, NULL_LONG);
            } else {
                outputValues.set(outIdx, curVal);
            }
        }

        @Override
        public void reset() {
            super.reset();
            charWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    public CharRollingSumOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new CharRollingSumOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits
        // region extra-copy-args
        // endregion extra-copy-args
        );
    }
}

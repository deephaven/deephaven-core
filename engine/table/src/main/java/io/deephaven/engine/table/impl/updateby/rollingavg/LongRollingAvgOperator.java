//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollingAvgOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class LongRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected LongChunk<? extends Values> influencerValuesChunk;
        protected LongRingBuffer longWindowValues;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            longWindowValues = new LongRingBuffer(BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            longWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asLongChunk();
        }

        @Override
        public void push(int pos, int count) {
            longWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final long val = influencerValuesChunk.get(pos + ii);
                longWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_LONG) {
                    if (curVal == NULL_DOUBLE) {
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
            Assert.geq(longWindowValues.size(), "longWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                long val = longWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_LONG) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (longWindowValues.isEmpty()) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = longWindowValues.size() - nullCount;
                if (count == 0) {
                    outputValues.set(outIdx, NULL_DOUBLE);
                } else {
                    outputValues.set(outIdx, curVal / (double) count);
                }
            }
        }

        @Override
        public void reset() {
            super.reset();
            longWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public LongRollingAvgOperator(
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
        return new LongRollingAvgOperator(
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

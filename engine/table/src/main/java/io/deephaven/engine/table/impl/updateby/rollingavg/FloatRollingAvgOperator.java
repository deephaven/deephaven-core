//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.AggregatingFloatRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int PAIRWISE_BUFFER_INITIAL_SIZE = 64;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected FloatChunk<? extends Values> floatInfluencerValuesChunk;
        protected AggregatingFloatRingBuffer aggSum;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            aggSum = new AggregatingFloatRingBuffer(PAIRWISE_BUFFER_INITIAL_SIZE, 0.0f, (a, b) -> {
                if (a == NULL_FLOAT) {
                    return b;
                } else if (b == NULL_FLOAT) {
                    return a;
                }
                return a + b;
            });
        }

        @Override
        public void close() {
            super.close();
            aggSum = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            floatInfluencerValuesChunk = valueChunks[0].asFloatChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final float val = floatInfluencerValuesChunk.get(pos + ii);
                aggSum.addUnsafe(val);

                if (val == NULL_FLOAT) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggSum.size(), "floatWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                float val = aggSum.removeUnsafe();

                if (val == NULL_FLOAT) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggSum.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = aggSum.size() - nullCount;
                if (count == 0) {
                    outputValues.set(outIdx, NULL_DOUBLE);
                } else {
                    outputValues.set(outIdx, aggSum.evaluate() / (double) count);
                }
            }
        }

        @Override
        public void reset() {
            super.reset();
            aggSum.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public FloatRollingAvgOperator(
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
        return new FloatRollingAvgOperator(
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

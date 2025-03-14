//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRollingSumOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingSumOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_SIZE = 64;

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected AggregatingDoubleRingBuffer aggSum;

        protected Context(final int chunkSize) {
            super(chunkSize);
            aggSum = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_SIZE,
                    0,
                    Double::sum, // tree function
                    (a, b) -> { // value function
                        if (a == NULL_DOUBLE && b == NULL_DOUBLE) {
                            return 0; // identity val
                        } else if (a == NULL_DOUBLE) {
                            return b;
                        } else if (b == NULL_DOUBLE) {
                            return a;
                        }
                        return a + b;
                    },
                    true);
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            doubleInfluencerValuesChunk = valueChunks[0].asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final double val = doubleInfluencerValuesChunk.get(pos + ii);

                if (val == NULL_DOUBLE) {
                    nullCount++;
                    aggSum.addUnsafe(NULL_DOUBLE);
                } else {
                    aggSum.addUnsafe(val);
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggSum.size(), "aggSum.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = aggSum.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggSum.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                outputValues.set(outIdx, aggSum.evaluate());
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
        return new Context(affectedChunkSize);
    }

    public DoubleRollingSumOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
    }

    @Override
    public UpdateByOperator copy() {
        return new DoubleRollingSumOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits);
    }
}

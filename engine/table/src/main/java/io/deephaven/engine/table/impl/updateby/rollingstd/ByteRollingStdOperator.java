//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollingStdOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingstd;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class ByteRollingStdOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected ByteChunk<? extends Values> influencerValuesChunk;
        protected AggregatingDoubleRingBuffer valueBuffer;
        protected AggregatingDoubleRingBuffer valueSquareBuffer;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            valueBuffer = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0,
                    Double::sum,
                    ((a, b) -> {
                        if (a == NULL_DOUBLE && b == NULL_DOUBLE) {
                            return 0.0; // identity value
                        } else if (a == NULL_DOUBLE) {
                            return b;
                        } else if (b == NULL_DOUBLE) {
                            return a;
                        }
                        return a + b;
                    }));
            valueSquareBuffer = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0,
                    Double::sum,
                    ((a, b) -> {
                        if (a == NULL_DOUBLE && b == NULL_DOUBLE) {
                            return 0.0; // identity value
                        } else if (a == NULL_DOUBLE) {
                            return b;
                        } else if (b == NULL_DOUBLE) {
                            return a;
                        }
                        return a + b;
                    }));
        }

        @Override
        public void close() {
            super.close();
            valueBuffer = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asByteChunk();
        }

        @Override
        public void push(int pos, int count) {
            valueBuffer.ensureRemaining(count);
            valueSquareBuffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final byte val = influencerValuesChunk.get(pos + ii);

                if (val != nullValue) {
                    // Add the value and its square to the buffers.
                    valueBuffer.addUnsafe(val);
                    valueSquareBuffer.addUnsafe((double) val * val);
                } else {
                    // Add null to the buffers and increment the count.
                    valueBuffer.addUnsafe(NULL_DOUBLE);
                    valueSquareBuffer.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(valueBuffer.size(), "valueBuffer.size()", count);
            Assert.geq(valueSquareBuffer.size(), "valueSquareBuffer.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final double val = valueBuffer.removeUnsafe();
                final double valSquare = valueSquareBuffer.removeUnsafe();

                if (val == NULL_DOUBLE || valSquare == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (valueBuffer.isEmpty()) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                if (nullCount == valueBuffer.size()) {
                    outputValues.set(outIdx, NULL_DOUBLE);
                    return;
                }

                final int count = valueBuffer.size() - nullCount;

                if (count <= 1) {
                    outputValues.set(outIdx, Double.NaN);
                    return;
                }

                final double valueSquareSum = valueSquareBuffer.evaluate();
                final double valueSum = valueBuffer.evaluate();

                if (Double.isNaN(valueSquareSum)
                        || Double.isNaN(valueSum)
                        || Double.isInfinite(valueSquareSum)
                        || Double.isInfinite(valueSum)) {
                    outputValues.set(outIdx, Double.NaN);
                    return;
                }

                // Perform the calculation in a way that minimizes the impact of floating point error.
                final double eps = Math.ulp(valueSquareSum);
                final double vs2bar = valueSum * (valueSum / count);
                final double delta = valueSquareSum - vs2bar;
                final double rel_eps = delta / eps;

                // Assign zero when the variance is leq the floating point error.
                final double variance = Math.abs(rel_eps) > 1.0 ? delta / (count - 1) : 0.0;

                final double std = Math.sqrt(variance);

                outputValues.set(outIdx, std);
            }
        }

        @Override
        public void reset() {
            super.reset();
            valueBuffer.clear();
            valueSquareBuffer.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public ByteRollingStdOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits
    // region extra-constructor-args
            ,final byte nullValue
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new ByteRollingStdOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits
        // region extra-copy-args
                , nullValue
        // endregion extra-copy-args
        );
    }
}

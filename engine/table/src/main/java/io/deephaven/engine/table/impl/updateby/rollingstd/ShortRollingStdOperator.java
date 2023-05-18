/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingStdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingstd;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class ShortRollingStdOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected ShortChunk<? extends Values> influencerValuesChunk;
        protected AggregatingDoubleRingBuffer valueBuffer;
        protected AggregatingDoubleRingBuffer valueSquareBuffer;

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
            influencerValuesChunk = valueChunks[0].asShortChunk();
        }

        @Override
        public void push(int pos, int count) {
            valueBuffer.ensureRemaining(count);
            valueSquareBuffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final short val = influencerValuesChunk.get(pos + ii);

                if (val != NULL_SHORT) {
                    // Add the value and its square to the buffers.
                    valueBuffer.addUnsafe(val);
                    valueSquareBuffer.addUnsafe((double)val * val);
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
            if (valueBuffer.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = valueBuffer.size() - nullCount;
                final double valueSquareSum = valueSquareBuffer.evaluate();
                final double valueSum = valueBuffer.evaluate();

                final double variance = valueSquareSum / (count - 1) - valueSum * valueSum / count / (count - 1);
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

    public ShortRollingStdOperator(@NotNull final MatchPair pair,
                                  @NotNull final String[] affectingColumns,
                                  @Nullable final RowRedirection rowRedirection,
                                  @Nullable final String timestampColumnName,
                                  final long reverseWindowScaleUnits,
                                  final long forwardWindowScaleUnits
                                  // region extra-constructor-args
                                  // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        // region constructor
        // endregion constructor
    }
}

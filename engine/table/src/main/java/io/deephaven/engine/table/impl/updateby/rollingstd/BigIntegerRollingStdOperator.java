package io.deephaven.engine.table.impl.updateby.rollingstd;

import io.deephaven.base.ringbuffer.AggregatingObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public class BigIntegerRollingStdOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    private final MathContext mathContext;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> influencerValuesChunk;
        protected AggregatingObjectRingBuffer<BigDecimal> valueBuffer;
        protected AggregatingObjectRingBuffer<BigDecimal> valueSquareBuffer;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            valueBuffer = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, BigDecimal.ZERO,
                    BigDecimal::add,
                    ((a, b) -> {
                        if (a == null && b == null) {
                            return BigDecimal.ZERO; // identity value
                        } else if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        }
                        return a.add(b);
                    }));
            valueSquareBuffer = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, BigDecimal.ZERO,
                    BigDecimal::add,
                    ((a, b) -> {
                        if (a == null && b == null) {
                            return BigDecimal.ZERO; // identity value
                        } else if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        }
                        return a.add(b);
                    }));
        }

        @Override
        public void close() {
            super.close();
            valueBuffer = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            valueBuffer.ensureRemaining(count);
            valueSquareBuffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final BigInteger val = influencerValuesChunk.get(pos + ii);

                if (val != null) {
                    final BigDecimal decVal = new BigDecimal(val);
                    // Add the value and its square to the buffers.
                    valueBuffer.addUnsafe(decVal);
                    valueSquareBuffer.addUnsafe(decVal.multiply(decVal));
                } else {
                    // Add null to the buffers and increment the count.
                    valueBuffer.addUnsafe(null);
                    valueSquareBuffer.addUnsafe(null);
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(valueBuffer.size(), "valueBuffer.size()", count);
            Assert.geq(valueSquareBuffer.size(), "valueSquareBuffer.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final BigDecimal val = valueBuffer.removeUnsafe();
                final BigDecimal valSquare = valueSquareBuffer.removeUnsafe();

                if (val == null || valSquare == null) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (valueBuffer.size() == nullCount) {
                outputValues.set(outIdx, null);
            } else {
                final int count = valueBuffer.size() - nullCount;

                if (count <= 1) {
                    // Prevent divide by zero and return null.
                    outputValues.set(outIdx, null);
                    return;
                }

                final BigDecimal valueSquareSum = valueSquareBuffer.evaluate();
                final BigDecimal valueSum = valueBuffer.evaluate();

                final BigDecimal biCount = BigDecimal.valueOf(count);
                final BigDecimal biCountMinusOne = BigDecimal.valueOf(count - 1);

                final BigDecimal variance = valueSquareSum.divide(biCountMinusOne, mathContext)
                        .subtract(valueSum.multiply(valueSum, mathContext)
                                .divide(biCount, mathContext)
                                .divide(biCountMinusOne, mathContext)
                        );
                final BigDecimal std = variance.sqrt(mathContext);

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

    public BigIntegerRollingStdOperator(@NotNull final MatchPair pair,
                                        @NotNull final String[] affectingColumns,
                                        @Nullable final RowRedirection rowRedirection,
                                        @Nullable final String timestampColumnName,
                                        final long reverseWindowScaleUnits,
                                        final long forwardWindowScaleUnits,
                                        final MathContext mathContext
                                        // region extra-constructor-args
                                        // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true, BigDecimal.class);
        this.mathContext = mathContext;
        // region constructor
        // endregion constructor
    }
}

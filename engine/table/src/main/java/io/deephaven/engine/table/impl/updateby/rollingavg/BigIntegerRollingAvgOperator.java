package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.RingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * NOTE: The output column for RollingAvg(BigInteger) is BigDecimal
 */
public final class BigIntegerRollingAvgOperator extends BaseObjectUpdateByOperator<BigDecimal> {

    private static final int RING_BUFFER_INITIAL_CAPACITY = 128;
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigInteger> objectWindowValues;

        protected Context(final int chunkSize) {
            super(chunkSize);
            objectWindowValues = new RingBuffer<>(RING_BUFFER_INITIAL_CAPACITY);
        }

        @Override
        public void close() {
            super.close();
            objectWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectInfluencerValuesChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectInfluencerValuesChunk.get(pos + ii);
                objectWindowValues.add(val);

                // increase the running sum
                if (val != null) {
                    final BigDecimal decVal = new BigDecimal(val);
                    if (curVal == null) {
                        curVal = decVal;
                    } else {
                        curVal = curVal.add(decVal);
                    }
                } else {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectWindowValues.remove();

                // reduce the running sum
                if (val != null) {
                    final BigDecimal decVal = new BigDecimal(val);
                    curVal = curVal.subtract(decVal);
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (objectWindowValues.size() == nullCount) {
                outputValues.set(outIdx, null);
                curVal = null;
            } else {
                final BigDecimal count = new BigDecimal(objectWindowValues.size() - nullCount);
                outputValues.set(outIdx, curVal.divide(count, mathContext));
            }
        }

        @Override
        public void reset() {
            super.reset();
            objectWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public BigIntegerRollingAvgOperator(@NotNull final MatchPair pair,
                                        @NotNull final String[] affectingColumns,
                                        @Nullable final RowRedirection rowRedirection,
                                        @Nullable final String timestampColumnName,
                                        final long reverseWindowScaleUnits,
                                        final long forwardWindowScaleUnits,
                                        @NotNull final MathContext mathContext
                                        // region extra-constructor-args
                                        // endregion extra-constructor-args
                                        ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true, BigDecimal.class);
        this.mathContext = mathContext;
        // region constructor
        // endregion constructor        
    }
}

package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.RingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigDecimal> {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseWindowedObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<BigDecimal, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigDecimal> objectWindowValues;

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
        public void push(long key, int pos, int count) {
            for (int ii = 0; ii < count; ii++) {
                BigDecimal val = objectInfluencerValuesChunk.get(pos + ii);
                objectWindowValues.add(val);

                // increase the running sum
                if (val != null) {
                    if (curVal == null) {
                        curVal = val;
                    } else {
                        curVal = curVal.add(val, mathContext);
                    }
                } else {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            for (int ii = 0; ii < count; ii++) {
                BigDecimal val = objectWindowValues.remove();

                // reduce the running sum
                if (val != null) {
                    curVal = curVal.subtract(val, mathContext);
                } else {
                    nullCount--;

                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (objectWindowValues.size() == nullCount) {
                outputValues.set(outIdx, null);
            } else {
                outputValues.set(outIdx, curVal);
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public BigDecimalRollingSumOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits,
                rowRedirection, BigDecimal.class);
        this.mathContext = mathContext;
    }
}

package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.RingBuffer;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public final class BigDecimalRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseWindowedObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<BigDecimal, Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigDecimal> objectWindowValues;

        protected Context(final int chunkSize) {
            super(chunkSize);
            objectWindowValues = new RingBuffer<>(512);
        }

        @Override
        public void close() {
            super.close();
            objectWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            objectInfluencerValuesChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(long key, int pos) {
            BigDecimal val = objectInfluencerValuesChunk.get(pos);
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

        @Override
        public void pop() {
            BigDecimal val = val = objectWindowValues.remove();

            // reduce the running sum
            if (val != null) {
                curVal = curVal.subtract(val, mathContext);
            } else {
                nullCount--;
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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public BigDecimalRollingSumOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits,
                redirContext, BigDecimal.class);
        this.mathContext = mathContext;
    }
}

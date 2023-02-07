package io.deephaven.engine.table.impl.updateby.rollingsum;

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
import java.math.MathContext;

public final class BigDecimalRollingSumOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        protected ObjectChunk<BigDecimal, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigDecimal> objectWindowValues;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
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
                curVal = null;
            } else {
                outputValues.set(outIdx, curVal);
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public BigDecimalRollingSumOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true, BigDecimal.class);
        this.mathContext = mathContext;
    }
}

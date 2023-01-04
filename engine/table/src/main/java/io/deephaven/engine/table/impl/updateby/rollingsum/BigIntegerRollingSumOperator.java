package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.RingBuffer;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseWindowedObjectUpdateByOperator<BigInteger>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigInteger> objectWindowValues;

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
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectInfluencerValuesChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(long key, int pos) {
            BigInteger val = objectInfluencerValuesChunk.get(pos);
            objectWindowValues.add(val);

            // increase the running sum
            if (val != null) {
                if (curVal == null) {
                    curVal = val;
                } else {
                    curVal = curVal.add(val);
                }
            } else {
                nullCount++;
            }
        }

        @Override
        public void pop() {
            BigInteger val = objectWindowValues.remove();

            // reduce the running sum
            if (val != null) {
                curVal = curVal.subtract(val);
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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public BigIntegerRollingSumOperator(@NotNull final MatchPair pair,
                                        @NotNull final String[] affectingColumns,
                                        @NotNull final OperationControl control,
                                        @Nullable final String timestampColumnName,
                                        final long reverseTimeScaleUnits,
                                        final long forwardTimeScaleUnits,
                                        @Nullable final WritableRowRedirection rowRedirection
                                        // region extra-constructor-args
                                        // endregion extra-constructor-args
                                        ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, rowRedirection, BigInteger.class);
        // region constructor
        // endregion constructor        
    }
}

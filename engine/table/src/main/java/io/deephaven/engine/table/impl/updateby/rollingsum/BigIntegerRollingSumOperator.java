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

import java.math.BigInteger;

public final class BigIntegerRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigInteger> {

    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;

    protected class Context extends BaseWindowedObjectUpdateByOperator<BigInteger>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigInteger> objectWindowValues;

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
        public void push(long key, int pos, int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectInfluencerValuesChunk.get(pos + ii);
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
        }

        @Override
        public void pop(int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectWindowValues.remove();

                // reduce the running sum
                if (val != null) {
                    curVal = curVal.subtract(val);
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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public BigIntegerRollingSumOperator(@NotNull final MatchPair pair,
                                        @NotNull final String[] affectingColumns,
                                        @Nullable final String timestampColumnName,
                                        final long reverseWindowScaleUnits,
                                        final long forwardWindowScaleUnits,
                                        @Nullable final RowRedirection rowRedirection
                                        // region extra-constructor-args
                                        // endregion extra-constructor-args
                                        ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, rowRedirection, BigInteger.class);
        // region constructor
        // endregion constructor        
    }
}

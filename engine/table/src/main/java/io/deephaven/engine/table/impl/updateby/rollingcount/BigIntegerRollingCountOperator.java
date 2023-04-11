package io.deephaven.engine.table.impl.updateby.rollingcount;

import io.deephaven.base.ringbuffer.ObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public class BigIntegerRollingCountOperator extends BaseLongUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected ObjectChunk<BigInteger, ? extends Values> influencerValuesChunk;
        protected ObjectRingBuffer<BigInteger> buffer;
        protected boolean evaluationNeeded;

        protected Context(final int chunkSize) {
            super(chunkSize);
            buffer = new ObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, true);
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            influencerValuesChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final BigInteger val = influencerValuesChunk.get(pos + ii);
                buffer.addUnsafe(val);

                if (val == null) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(buffer.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final BigInteger val = buffer.removeUnsafe();

                if (val == null) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            curVal = buffer.size() - nullCount;
            outputValues.set(outIdx, curVal);
            evaluationNeeded = false;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.clear();
            evaluationNeeded = false;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public BigIntegerRollingCountOperator(@NotNull final MatchPair pair,
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

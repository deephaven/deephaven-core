package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ShortRollingSumOperator extends BaseLongUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected ShortChunk<? extends Values> shortInfluencerValuesChunk;
        protected ShortRingBuffer shortWindowValues;


        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            shortWindowValues = new ShortRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            shortWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            shortInfluencerValuesChunk = valuesChunk.asShortChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            shortWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                short val = shortInfluencerValuesChunk.get(pos + ii);
                shortWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_SHORT) {
                    if (curVal == NULL_LONG) {
                        curVal = val;
                    } else {
                        curVal += val;
                    }
                } else {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(shortWindowValues.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                short val = shortWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_SHORT) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (shortWindowValues.size() == nullCount) {
                outputValues.set(outIdx, NULL_LONG);
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

    public ShortRollingSumOperator(@NotNull final MatchPair pair,
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

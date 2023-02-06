/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class LongRollingSumOperator extends BaseLongUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected LongChunk<? extends Values> longInfluencerValuesChunk;
        protected LongRingBuffer longWindowValues;


        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            longWindowValues = new LongRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            longWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            longInfluencerValuesChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            longWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                long val = longInfluencerValuesChunk.get(pos + ii);
                longWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_LONG) {
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
            Assert.geq(longWindowValues.size(), "longWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                long val = longWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_LONG) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (longWindowValues.size() == nullCount) {
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

    public LongRollingSumOperator(@NotNull final MatchPair pair,
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

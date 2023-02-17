/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class IntRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected IntChunk<? extends Values> intInfluencerValuesChunk;
        protected IntRingBuffer intWindowValues;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            intWindowValues = new IntRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            intWindowValues = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            intInfluencerValuesChunk = valuesChunk.asIntChunk();
        }

        @Override
        public void push(int pos, int count) {
            intWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final int val = intInfluencerValuesChunk.get(pos + ii);
                intWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_INT) {
                    if (curVal == NULL_DOUBLE) {
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
            Assert.geq(intWindowValues.size(), "intWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                int val = intWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_INT) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (intWindowValues.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = intWindowValues.size() - nullCount;
                outputValues.set(outIdx, curVal / (double)count);
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public IntRollingAvgOperator(@NotNull final MatchPair pair,
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

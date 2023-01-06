/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class IntRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        protected IntChunk<? extends Values> intInfluencerValuesChunk;
        protected IntRingBuffer intWindowValues;


        protected Context(int chunkSize) {
            super(chunkSize);
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
        public void push(long key, int pos) {
            int val = intInfluencerValuesChunk.get(pos);
            intWindowValues.add(val);

            // increase the running sum
            if (val != NULL_INT) {
                if (curVal == NULL_LONG) {
                    curVal = val;
                } else {
                    curVal += val;
                }
            } else {
                nullCount++;
            }
        }

        @Override
        public void pop() {
            int val = intWindowValues.remove();

            // reduce the running sum
            if (val != NULL_INT) {
                curVal -= val;
            } else {
                nullCount--;
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (intWindowValues.size() == nullCount) {
                outputValues.set(outIdx, NULL_LONG);
            } else {
                outputValues.set(outIdx, curVal);
            }
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public IntRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @Nullable final RowRedirection rowRedirection
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, rowRedirection);
        // region constructor
        // endregion constructor
    }
}

package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.ShortRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ShortRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        protected ShortChunk<? extends Values> shortInfluencerValuesChunk;
        protected ShortRingBuffer shortWindowValues;


        protected Context(int chunkSize) {
            super(chunkSize);
            shortWindowValues = new ShortRingBuffer(512, true);
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
        public void push(long key, int pos) {
            short val = shortInfluencerValuesChunk.get(pos);
            shortWindowValues.add(val);

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

        @Override
        public void pop() {
            short val = shortWindowValues.remove();

            // reduce the running sum
            if (val != NULL_SHORT) {
                curVal -= val;
            } else {
                nullCount--;
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
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?>[] inputSourceArr) {
        return new Context(chunkSize);
    }

    public ShortRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        // region constructor
        // endregion constructor
    }
}

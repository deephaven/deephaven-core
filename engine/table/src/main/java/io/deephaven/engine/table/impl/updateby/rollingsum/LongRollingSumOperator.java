/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class LongRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        protected LongChunk<Values> longInfluencerValuesChunk;
        protected LongRingBuffer longWindowValues;


        protected Context(int chunkSize) {
            super(chunkSize);
            longWindowValues = new LongRingBuffer(512, true);
        }

        @Override
        public void close() {
            super.close();
            longWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            longInfluencerValuesChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(long key, int pos) {
            long val = longInfluencerValuesChunk.get(pos);
            longWindowValues.add(val);

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

        @Override
        public void pop() {
            long val = longWindowValues.remove();

            // reduce the running sum
            if (val != NULL_LONG) {
                curVal -= val;
            } else {
                nullCount--;
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
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public LongRollingSumOperator(@NotNull final MatchPair pair,
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

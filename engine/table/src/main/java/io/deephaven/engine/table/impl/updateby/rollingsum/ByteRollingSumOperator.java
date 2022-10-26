/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        protected ByteChunk<? extends Values> byteInfluencerValuesChunk;
        protected ByteRingBuffer byteWindowValues;


        protected Context(int chunkSize) {
            super(chunkSize);
            byteWindowValues = new ByteRingBuffer(512, true);
        }

        @Override
        public void close() {
            super.close();
            byteWindowValues = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            byteInfluencerValuesChunk = valuesChunk.asByteChunk();
        }

        @Override
        public void push(long key, int pos) {
            byte val = byteInfluencerValuesChunk.get(pos);
            byteWindowValues.add(val);

            // increase the running sum
            if (val != NULL_BYTE) {
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
            byte val = byteWindowValues.remove();

            // reduce the running sum
            if (val != NULL_BYTE) {
                curVal -= val;
            } else {
                nullCount--;
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (byteWindowValues.size() == nullCount) {
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

    public ByteRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                               ,final byte nullValue
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }
}

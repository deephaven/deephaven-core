/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteRollingSumOperator extends BaseWindowedLongUpdateByOperator {
    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseWindowedLongUpdateByOperator.Context {
        protected ByteChunk<? extends Values> byteInfluencerValuesChunk;
        protected ByteRingBuffer byteWindowValues;


        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            byteWindowValues = new ByteRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
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
        public void push(long key, int pos, int count) {
            byteWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                byte val = byteInfluencerValuesChunk.get(pos + ii);
                byteWindowValues.addUnsafe(val);

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
        }

        @Override
        public void pop(int count) {
            Assert.geq(byteWindowValues.size(), "byteWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                byte val = byteWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_BYTE) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
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
    public UpdateContext makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public ByteRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @Nullable final String timestampColumnName,
                                   final long reverseWindowScaleUnits,
                                   final long forwardWindowScaleUnits,
                                   @Nullable final RowRedirection rowRedirection
                                   // region extra-constructor-args
                               ,final byte nullValue
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, rowRedirection);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }
}

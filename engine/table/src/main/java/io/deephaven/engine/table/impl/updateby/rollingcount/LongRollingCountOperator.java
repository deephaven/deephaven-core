/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingCountOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingcount;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongRollingCountOperator extends BaseLongUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected LongChunk<? extends Values> influencerValuesChunk;
        protected LongRingBuffer buffer;
        protected boolean evaluationNeeded;

        protected Context(final int chunkSize) {
            super(chunkSize);
            buffer = new LongRingBuffer(BUFFER_INITIAL_CAPACITY, true);
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            influencerValuesChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(int pos, int count) {
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final long val = influencerValuesChunk.get(pos + ii);
                buffer.addUnsafe(val);

                if (val == NULL_LONG) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(buffer.size(), "longWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final long val = buffer.removeUnsafe();

                if (val == NULL_LONG) {
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

    public LongRollingCountOperator(@NotNull final MatchPair pair,
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

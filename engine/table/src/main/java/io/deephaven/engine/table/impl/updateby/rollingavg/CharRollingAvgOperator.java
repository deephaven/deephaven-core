package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.CharRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class CharRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected CharChunk<? extends Values> influencerValuesChunk;
        protected CharRingBuffer charWindowValues;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            charWindowValues = new CharRingBuffer(BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            charWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asCharChunk();
        }

        @Override
        public void push(int pos, int count) {
            charWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final char val = influencerValuesChunk.get(pos + ii);
                charWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != NULL_CHAR) {
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
            Assert.geq(charWindowValues.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                char val = charWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != NULL_CHAR) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (charWindowValues.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = charWindowValues.size() - nullCount;
                if (count == 0) {
                    outputValues.set(outIdx, Double.NaN);
                } else {
                    outputValues.set(outIdx, curVal / (double)count);
                }
            }
        }

        @Override
        public void reset() {
            super.reset();
            charWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public CharRollingAvgOperator(@NotNull final MatchPair pair,
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

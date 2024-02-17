/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class IntRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected IntChunk<? extends Values> influencerValuesChunk;
        protected IntRingBuffer intWindowValues;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            intWindowValues = new IntRingBuffer(BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            intWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asIntChunk();
        }

        @Override
        public void push(int pos, int count) {
            intWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final int val = influencerValuesChunk.get(pos + ii);
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
            intWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
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

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected ByteChunk<? extends Values> influencerValuesChunk;
        protected ByteRingBuffer byteWindowValues;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            byteWindowValues = new ByteRingBuffer(BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            byteWindowValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asByteChunk();
        }

        @Override
        public void push(int pos, int count) {
            byteWindowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final byte val = influencerValuesChunk.get(pos + ii);
                byteWindowValues.addUnsafe(val);

                // increase the running sum
                if (val != nullValue) {
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
            Assert.geq(byteWindowValues.size(), "byteWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                byte val = byteWindowValues.removeUnsafe();

                // reduce the running sum
                if (val != nullValue) {
                    curVal -= val;
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (byteWindowValues.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = byteWindowValues.size() - nullCount;
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
            byteWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public ByteRollingAvgOperator(@NotNull final MatchPair pair,
                                  @NotNull final String[] affectingColumns,
                                  @Nullable final RowRedirection rowRedirection,
                                  @Nullable final String timestampColumnName,
                                  final long reverseWindowScaleUnits,
                                  final long forwardWindowScaleUnits
                                  // region extra-constructor-args
                               ,final byte nullValue
                                  // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }
}

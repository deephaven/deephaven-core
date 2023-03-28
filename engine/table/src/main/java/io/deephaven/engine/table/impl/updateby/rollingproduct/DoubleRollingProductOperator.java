/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingProductOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingproduct;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class DoubleRollingProductOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_SIZE = 64;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected AggregatingDoubleRingBuffer buffer;

        private int zeroCount;

        protected Context(final int chunkSize) {
            super(chunkSize);
            buffer = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_SIZE,
                    1L,
                    (a, b) -> a * b, // tree function
                    (a, b) -> { // value function
                        if (a == NULL_DOUBLE && b == NULL_DOUBLE) {
                            return 1L; // identity val
                        } else if (a == NULL_DOUBLE) {
                            return b;
                        } else if (b == NULL_DOUBLE) {
                            return  a;
                        }
                        return a * b;
                    },
                    true);
            zeroCount = 0;
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleInfluencerValuesChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                double val = doubleInfluencerValuesChunk.get(pos + ii);

                if (val == NULL_DOUBLE) {
                    buffer.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                } else {
                    buffer.addUnsafe(val);
                    if (val == 0) {
                        zeroCount++;
                    }
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(buffer.size(), "buffer.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = buffer.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                } else if (val == 0) {
                    --zeroCount;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (buffer.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                outputValues.set(outIdx, zeroCount > 0 ? 0.0 : buffer.evaluate());
            }
        }

        @Override
        public void reset() {
            super.reset();
            zeroCount = 0;
            buffer.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public DoubleRollingProductOperator(@NotNull final MatchPair pair,
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

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingminmax;

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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingMinMaxOperator extends BaseDoubleUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected AggregatingDoubleRingBuffer aggMinMax;

        protected Context(final int chunkSize) {
            super(chunkSize);
            if (isMax) {
                aggMinMax = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, Double.MIN_VALUE, (a, b) -> {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return a;
                    }
                    return (double)Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, Double.MAX_VALUE, (a, b) -> {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return a;
                    }
                    return (double)Math.min(a, b);
                });
            }
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }


        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleInfluencerValuesChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                double val = doubleInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_DOUBLE) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "doubleWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = aggMinMax.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                outputValues.set(outIdx, aggMinMax.evaluate());
            }
        }

        @Override
        public void reset() {
            super.reset();
            aggMinMax.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public DoubleRollingMinMaxOperator(@NotNull final MatchPair pair,
                                      @NotNull final String[] affectingColumns,
                                      @Nullable final RowRedirection rowRedirection,
                                      @Nullable final String timestampColumnName,
                                      final long reverseWindowScaleUnits,
                                      final long forwardWindowScaleUnits,
                                      final boolean isMax
                                      // region extra-constructor-args
                                      // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }
}

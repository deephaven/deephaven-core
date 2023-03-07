/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingLongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class LongRollingMinMaxOperator extends BaseLongUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected LongChunk<? extends Values> longInfluencerValuesChunk;
        protected AggregatingLongRingBuffer aggMinMax;

        protected Context(final int chunkSize) {
            super(chunkSize);
            if (isMax) {
                aggMinMax = new AggregatingLongRingBuffer(BUFFER_INITIAL_CAPACITY, Long.MIN_VALUE, (a, b) -> {
                    if (a == NULL_LONG) {
                        return b;
                    } else if (b == NULL_LONG) {
                        return a;
                    }
                    return (long)Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingLongRingBuffer(BUFFER_INITIAL_CAPACITY, Long.MAX_VALUE, (a, b) -> {
                    if (a == NULL_LONG) {
                        return b;
                    } else if (b == NULL_LONG) {
                        return a;
                    }
                    return (long)Math.min(a, b);
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
            longInfluencerValuesChunk = valuesChunk.asLongChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                long val = longInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_LONG) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "longWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                long val = aggMinMax.removeUnsafe();

                if (val == NULL_LONG) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                outputValues.set(outIdx, NULL_LONG);
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

    public LongRollingMinMaxOperator(@NotNull final MatchPair pair,
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

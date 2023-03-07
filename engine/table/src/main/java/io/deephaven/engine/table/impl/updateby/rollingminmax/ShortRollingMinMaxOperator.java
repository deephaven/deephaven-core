package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingShortRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseShortUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortRollingMinMaxOperator extends BaseShortUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseShortUpdateByOperator.Context {
        protected ShortChunk<? extends Values> shortInfluencerValuesChunk;
        protected AggregatingShortRingBuffer aggMinMax;


        protected Context(final int chunkSize) {
            super(chunkSize);
            if (isMax) {
                aggMinMax = new AggregatingShortRingBuffer(BUFFER_INITIAL_CAPACITY, Short.MIN_VALUE, (a, b) -> {
                    if (a == NULL_SHORT) {
                        return b;
                    } else if (b == NULL_SHORT) {
                        return a;
                    }
                    return (short)Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingShortRingBuffer(BUFFER_INITIAL_CAPACITY, Short.MAX_VALUE, (a, b) -> {
                    if (a == NULL_SHORT) {
                        return b;
                    } else if (b == NULL_SHORT) {
                        return a;
                    }
                    return (short)Math.min(a, b);
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
            shortInfluencerValuesChunk = valuesChunk.asShortChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                short val = shortInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_SHORT) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                short val = aggMinMax.removeUnsafe();

                if (val == NULL_SHORT) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                outputValues.set(outIdx, NULL_SHORT);
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

    public ShortRollingMinMaxOperator(@NotNull final MatchPair pair,
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

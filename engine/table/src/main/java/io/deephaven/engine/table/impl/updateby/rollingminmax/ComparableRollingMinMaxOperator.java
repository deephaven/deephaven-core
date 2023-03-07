package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ComparableRollingMinMaxOperator<T extends Comparable<T>> extends BaseObjectUpdateByOperator<T> {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        protected ObjectChunk<T, ? extends Values> objectInfluencerValuesChunk;
        protected AggregatingObjectRingBuffer<T> aggMinMax;

        protected Context(final int chunkSize) {
            super(chunkSize);
            if (isMax) {
                aggMinMax = new AggregatingObjectRingBuffer<T>(BUFFER_INITIAL_CAPACITY, null, (a, b) -> {
                    if (a == null) {
                        return b;
                    } else if (b == null) {
                        return a;
                    }
                    return a.compareTo(b) > 0 ? a : b;
                });
            } else {
                aggMinMax = new AggregatingObjectRingBuffer<T>(BUFFER_INITIAL_CAPACITY, null, (a, b) -> {
                    if (a == null) {
                        return b;
                    } else if (b == null) {
                        return a;
                    }
                    return a.compareTo(b) < 0 ? a : b;
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
            objectInfluencerValuesChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                T val = objectInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == null) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                T val = aggMinMax.removeUnsafe();

                if (val == null) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                outputValues.set(outIdx, null);
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

    public ComparableRollingMinMaxOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final boolean isMax
            // region extra-constructor-args
            , final Class colType
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true, colType);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }
}

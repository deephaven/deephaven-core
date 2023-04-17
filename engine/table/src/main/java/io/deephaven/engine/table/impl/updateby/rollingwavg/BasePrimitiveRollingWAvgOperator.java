package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.cast.ToDoubleCast;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

abstract class BasePrimitiveRollingWAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    final String weightColumnName;
    final ColumnSource weightColumnSource;

    // region extra-fields
    // endregion extra-fields

    abstract class Context extends BaseDoubleUpdateByOperator.Context {
        DoubleChunk<? extends Values> influencerWeightValuesChunk;
        AggregatingDoubleRingBuffer windowValues;
        AggregatingDoubleRingBuffer windowWeightValues;

        ToDoubleCast weightCast;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            windowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return a;
                }
                return a + b;
            });
            windowWeightValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return a;
                }
                return a + b;
            });
            // Create a casting helper for the weight values.
            weightCast = ToDoubleCast.makeToDoubleCast(weightColumnSource.getChunkType(), influencerChunkSize);
        }

        @Override
        public void close() {
            super.close();
            weightCast.close();
            windowValues = null;
            windowWeightValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerWeightValuesChunk = weightCast.cast(valueChunks[1]);
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "windowValues.size()", count);
            Assert.geq(windowWeightValues.size(), "windowWeightValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final double val = windowValues.removeUnsafe();
                windowWeightValues.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (windowValues.size() == nullCount) {
                // Looks weird but returning NaN is consistent with Numeric#wavg and AggWAvg
                outputValues.set(outIdx, Double.NaN);
            } else {
                final double weightedValSum = windowValues.evaluate();
                final double weightSum = windowWeightValues.evaluate();

                // Divide by zero will result in NaN which is correct.
                outputValues.set(outIdx, weightedValSum / weightSum);
            }
        }

        @Override
        public void reset() {
            super.reset();
            windowValues.clear();
            windowWeightValues.clear();
        }
    }

    public BasePrimitiveRollingWAvgOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            @NotNull final String weightColumnName,
            @NotNull final ColumnSource weightColumnSource
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true);
        this.weightColumnName = weightColumnName;
        this.weightColumnSource = weightColumnSource;
        // region constructor
        // endregion constructor
    }

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    @Override
    protected String[] getInputColumnNames() {
        return new String[] {pair.rightColumn, weightColumnName};
    }
}

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortRollingWAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.cast.ToDoubleCast;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatRollingWAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    private final String weightColumnName;
    private final ChunkType weightChunkType;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        private final ToDoubleCast castKernel;
        protected FloatChunk<? extends Values> influencerValuesChunk;
        private DoubleChunk<? extends Values> influencerWeightValuesChunk;
        protected AggregatingDoubleRingBuffer windowValues;
        protected AggregatingDoubleRingBuffer windowWeightValues;

        protected Context(final int chunkSize, final int influencerChunkSize) {
            super(chunkSize);
            castKernel = ToDoubleCast.makeToDoubleCast(weightChunkType, influencerChunkSize);
            windowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return  a;
                }
                return a + b;
            });
            windowWeightValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
                if (a == NULL_DOUBLE) {
                    return b;
                } else if (b == NULL_DOUBLE) {
                    return  a;
                }
                return a + b;
            });
        }

        @Override
        public void close() {
            super.close();
            castKernel.close();
            windowValues = null;
            windowWeightValues = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asFloatChunk();
            influencerWeightValuesChunk = castKernel.cast(valueChunks[1]);
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);
            windowWeightValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final float val = influencerValuesChunk.get(pos + ii);
                final double weight = influencerWeightValuesChunk.get(pos + ii);

                if (val == NULL_FLOAT || weight == NULL_DOUBLE) {
                    windowValues.addUnsafe(NULL_DOUBLE);
                    windowWeightValues.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                } else {
                    // Compute the product and add to the agg buffer.
                    final double weightedVal = weight * val;
                    windowValues.addUnsafe(weightedVal);
                    windowWeightValues.addUnsafe(weight);
                }
            }
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
                // Looks weird but is consistent with Numeric#wavg and AggWAvg
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

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public FloatRollingWAvgOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection,
                                    @Nullable final String timestampColumnName,
                                    final long reverseWindowScaleUnits,
                                    final long forwardWindowScaleUnits,
                                    @NotNull final String weightColumnName,
                                    @NotNull final ChunkType weightChunkType
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.weightColumnName = weightColumnName;
        this.weightChunkType = weightChunkType;
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

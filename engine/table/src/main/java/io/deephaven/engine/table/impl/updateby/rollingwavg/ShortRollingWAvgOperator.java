package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class ShortRollingWAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    private final DoubleRollingWAvgRecorder recorder;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected ShortChunk<? extends Values> influencerValuesChunk;
        protected AggregatingDoubleRingBuffer windowValues;

        private DoubleRollingWAvgRecorder.Context recorderContext;

        protected Context(final int chunkSize) {
            super(chunkSize);
            windowValues = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, 0.0, (a, b) -> {
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
            windowValues = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            influencerValuesChunk = valuesChunk.asShortChunk();
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final short val = influencerValuesChunk.get(pos + ii);
                final double weight = recorder.getValue(recorderContext, pos + ii);

                if (val == NULL_SHORT || weight == NULL_DOUBLE) {
                    windowValues.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                } else {
                    // Compute the product and add to the agg buffer.
                    final double weightedVal = weight * val;
                    windowValues.addUnsafe(weightedVal);
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowValues.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final double weightedVal = windowValues.removeUnsafe();

                if (weightedVal == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (windowValues.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final double weightedValSum = windowValues.evaluate();
                final double weightSum = recorder.getSum(recorderContext, outIdx);

                // Divide by zero will result in NaN which is correct.
                outputValues.set(outIdx, weightedValSum / weightSum);
            }
        }

        @Override
        public void reset() {
            windowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    public ShortRollingWAvgOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @Nullable final RowRedirection rowRedirection,
                                    @Nullable final String timestampColumnName,
                                    final long reverseWindowScaleUnits,
                                    final long forwardWindowScaleUnits,
                                    final DoubleRollingWAvgRecorder recorder
                                    // region extra-constructor-args
                                    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.recorder = recorder;
        recorder.addAffectingColumnName(pair.rightColumn);
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
        String recorderColumnName = recorder.getWeightColumn();
        return new String[] {recorderColumnName, pair.rightColumn};
    }

    public void setRecorderContext(UpdateByOperator.Context opContext, DoubleRollingWAvgRecorder.Context recorderContext) {
        Context ctx = (Context)opContext;
        ctx.recorderContext = recorderContext;
    }
}

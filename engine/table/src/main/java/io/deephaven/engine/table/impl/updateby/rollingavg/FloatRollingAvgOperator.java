package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseFloatRingBuffer;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class FloatRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int PAIRWISE_BUFFER_INITIAL_SIZE = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected FloatChunk<? extends Values> floatInfluencerValuesChunk;
        protected PairwiseFloatRingBuffer floatPairwiseSum;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            floatPairwiseSum = new PairwiseFloatRingBuffer(PAIRWISE_BUFFER_INITIAL_SIZE, 0.0f, (a, b) -> {
                if (a == NULL_FLOAT) {
                    return b;
                } else if (b == NULL_FLOAT) {
                    return  a;
                }
                return a + b;
            });
        }

        @Override
        public void close() {
            super.close();
            floatPairwiseSum.close();
            floatPairwiseSum = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            floatInfluencerValuesChunk = valuesChunk.asFloatChunk();
        }

        @Override
        public void push(int pos, int count) {
            floatPairwiseSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final float val = floatInfluencerValuesChunk.get(pos + ii);
                floatPairwiseSum.pushUnsafe(val);

                if (val == NULL_FLOAT) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(floatPairwiseSum.size(), "floatWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                float val = floatPairwiseSum.popUnsafe();

                if (val == NULL_FLOAT) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (floatPairwiseSum.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = floatPairwiseSum.size() - nullCount;
                outputValues.set(outIdx, floatPairwiseSum.evaluate() / (double)count);
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public FloatRollingAvgOperator(@NotNull final MatchPair pair,
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

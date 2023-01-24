package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedFloatUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseFloatRingBuffer;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatRollingSumOperator extends BaseWindowedFloatUpdateByOperator {
    private static final int PAIRWISE_BUFFER_INITIAL_SIZE = 64;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedFloatUpdateByOperator.Context {
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
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            floatInfluencerValuesChunk = valuesChunk.asFloatChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            floatPairwiseSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                float val = floatInfluencerValuesChunk.get(pos + ii);
                floatPairwiseSum.pushUnsafe(val);

                if (val == NULL_FLOAT) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(floatPairwiseSum.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                float val = floatPairwiseSum.popUnsafe();

                if (val == NULL_FLOAT) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (floatPairwiseSum.size() == nullCount) {
                outputValues.set(outIdx, NULL_FLOAT);
            } else {
                outputValues.set(outIdx, floatPairwiseSum.evaluate());
            }
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public FloatRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @Nullable final String timestampColumnName,
                                   final long reverseWindowScaleUnits,
                                   final long forwardWindowScaleUnits,
                                   @Nullable final RowRedirection rowRedirection
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}

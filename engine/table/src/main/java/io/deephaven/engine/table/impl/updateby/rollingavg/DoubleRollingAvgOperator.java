/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRollingAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingavg;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseDoubleRingBuffer;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class DoubleRollingAvgOperator extends BaseDoubleUpdateByOperator {
    private static final int PAIRWISE_BUFFER_INITIAL_SIZE = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected PairwiseDoubleRingBuffer doublePairwiseSum;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
            doublePairwiseSum = new PairwiseDoubleRingBuffer(PAIRWISE_BUFFER_INITIAL_SIZE, 0.0f, (a, b) -> {
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
            doublePairwiseSum.close();
            doublePairwiseSum = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleInfluencerValuesChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            doublePairwiseSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final double val = doubleInfluencerValuesChunk.get(pos + ii);
                doublePairwiseSum.pushUnsafe(val);

                if (val == NULL_DOUBLE) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(doublePairwiseSum.size(), "doubleWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = doublePairwiseSum.popUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (doublePairwiseSum.size() == 0) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                final int count = doublePairwiseSum.size() - nullCount;
                outputValues.set(outIdx, doublePairwiseSum.evaluate() / (double)count);
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public DoubleRollingAvgOperator(@NotNull final MatchPair pair,
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

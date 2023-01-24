/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseDoubleRingBuffer;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingSumOperator extends BaseWindowedDoubleUpdateByOperator {
    private static final int PAIRWISE_BUFFER_INITIAL_SIZE = 64;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedDoubleUpdateByOperator.Context {
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
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleInfluencerValuesChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            doublePairwiseSum.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                double val = doubleInfluencerValuesChunk.get(pos + ii);
                doublePairwiseSum.pushUnsafe(val);

                if (val == NULL_DOUBLE) {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(doublePairwiseSum.size(), "shortWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = doublePairwiseSum.popUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (doublePairwiseSum.size() == nullCount) {
                outputValues.set(outIdx, NULL_DOUBLE);
            } else {
                outputValues.set(outIdx, doublePairwiseSum.evaluate());
            }
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

    public DoubleRollingSumOperator(@NotNull final MatchPair pair,
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

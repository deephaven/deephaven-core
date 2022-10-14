/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatRollingSumOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseDoubleRingBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingSumOperator extends BaseWindowedDoubleUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected PairwiseDoubleRingBuffer doublePairwiseSum;

        protected Context(final int chunkSize) {
            super(chunkSize);
            doublePairwiseSum = new PairwiseDoubleRingBuffer(64, 0.0f, new PairwiseDoubleRingBuffer.DoubleFunction() {
                @Override
                public double apply(double a, double b) {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return  a;
                    }
                    return a + b;
                }
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
        public void push(long key, int pos) {
            double val = doubleInfluencerValuesChunk.get(pos);

            doublePairwiseSum.push(val);
            if (val == NULL_DOUBLE) {
                nullCount++;
            }
        }

        @Override
        public void pop() {
            double val = doublePairwiseSum.pop();

            if (val == NULL_DOUBLE) {
                nullCount--;
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
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public DoubleRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollingMinMaxOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleRollingMinMaxOperator extends BaseDoubleUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        protected DoubleChunk<? extends Values> doubleInfluencerValuesChunk;
        protected AggregatingDoubleRingBuffer aggMinMax;
        protected boolean evaluationNeeded;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            if (isMax) {
                aggMinMax = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, NULL_DOUBLE, (a, b) -> {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return a;
                    }
                    return (double) Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingDoubleRingBuffer(BUFFER_INITIAL_CAPACITY, Double.MAX_VALUE, (a, b) -> {
                    if (a == NULL_DOUBLE) {
                        return b;
                    } else if (b == NULL_DOUBLE) {
                        return a;
                    }
                    return (double) Math.min(a, b);
                });
            }
            curVal = isMax ? NULL_DOUBLE : Double.MAX_VALUE;
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            doubleInfluencerValuesChunk = valueChunks[0].asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                double val = doubleInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_DOUBLE) {
                    nullCount++;
                } else if (curVal == NULL_DOUBLE) {
                    curVal = val;
                    evaluationNeeded = false;
                } else if (isMax && curVal < val) {
                    curVal = val;
                    // Can skip evaluation when we push a new extreme.
                    evaluationNeeded = false;
                } else if (!isMax && curVal > val) {
                    curVal = val;
                    // Can skip evaluation when we push a new extreme.
                    evaluationNeeded = false;
                }

            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "doubleWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                double val = aggMinMax.removeUnsafe();

                if (val == NULL_DOUBLE) {
                    nullCount--;
                } else {
                    // Only revaluate if we pop something equal to our current value. Otherwise we have perfect
                    // confidence that the min/max is still in the window.
                    if (curVal == val) {
                        evaluationNeeded = true;
                    }
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                curVal = NULL_DOUBLE;
            } else if (evaluationNeeded) {
                curVal = aggMinMax.evaluate();
            }
            outputValues.set(outIdx, curVal);
            evaluationNeeded = false;
        }

        @Override
        public void reset() {
            super.reset();
            aggMinMax.clear();
            curVal = isMax ? NULL_DOUBLE : Double.MAX_VALUE;
            evaluationNeeded = false;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public DoubleRollingMinMaxOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final boolean isMax
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new DoubleRollingMinMaxOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                isMax
        // region extra-copy-args
        // endregion extra-copy-args
        );
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingCharRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseCharUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class CharRollingMinMaxOperator extends BaseCharUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseCharUpdateByOperator.Context {
        protected CharChunk<? extends Values> charInfluencerValuesChunk;
        protected AggregatingCharRingBuffer aggMinMax;
        protected boolean evaluationNeeded;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            if (isMax) {
                aggMinMax = new AggregatingCharRingBuffer(BUFFER_INITIAL_CAPACITY, Character.MIN_VALUE, (a, b) -> {
                    if (a == NULL_CHAR) {
                        return b;
                    } else if (b == NULL_CHAR) {
                        return a;
                    }
                    return (char) Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingCharRingBuffer(BUFFER_INITIAL_CAPACITY, Character.MAX_VALUE, (a, b) -> {
                    if (a == NULL_CHAR) {
                        return b;
                    } else if (b == NULL_CHAR) {
                        return a;
                    }
                    return (char) Math.min(a, b);
                });
            }
            curVal = isMax ? Character.MIN_VALUE : Character.MAX_VALUE;
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            charInfluencerValuesChunk = valueChunks[0].asCharChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                char val = charInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_CHAR) {
                    nullCount++;
                } else if (curVal == NULL_CHAR) {
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
            Assert.geq(aggMinMax.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                char val = aggMinMax.removeUnsafe();

                if (val == NULL_CHAR) {
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
                curVal = NULL_CHAR;
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
            curVal = isMax ? Character.MIN_VALUE : Character.MAX_VALUE;
            evaluationNeeded = false;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public CharRollingMinMaxOperator(
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
        return new CharRollingMinMaxOperator(
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

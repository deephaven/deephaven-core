/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingIntRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseIntUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class IntRollingMinMaxOperator extends BaseIntUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseIntUpdateByOperator.Context {
        protected IntChunk<? extends Values> intInfluencerValuesChunk;
        protected AggregatingIntRingBuffer aggMinMax;
        protected boolean evaluationNeeded;

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            if (isMax) {
                aggMinMax = new AggregatingIntRingBuffer(BUFFER_INITIAL_CAPACITY, Integer.MIN_VALUE, (a, b) -> {
                    if (a == NULL_INT) {
                        return b;
                    } else if (b == NULL_INT) {
                        return a;
                    }
                    return (int)Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingIntRingBuffer(BUFFER_INITIAL_CAPACITY, Integer.MAX_VALUE, (a, b) -> {
                    if (a == NULL_INT) {
                        return b;
                    } else if (b == NULL_INT) {
                        return a;
                    }
                    return (int)Math.min(a, b);
                });
            }
            curVal = isMax ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            intInfluencerValuesChunk = valueChunks[0].asIntChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                int val = intInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_INT) {
                    nullCount++;
                } else if (curVal == NULL_INT) {
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
            Assert.geq(aggMinMax.size(), "intWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                int val = aggMinMax.removeUnsafe();

                if (val == NULL_INT) {
                    nullCount--;
                } else {
                    // Only revaluate if we pop something equal to our current value.  Otherwise we have perfect
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
                curVal = NULL_INT;
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
            curVal = isMax ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            evaluationNeeded = false;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public IntRollingMinMaxOperator(@NotNull final MatchPair pair,
                                     @NotNull final String[] affectingColumns,
                                     @Nullable final RowRedirection rowRedirection,
                                     @Nullable final String timestampColumnName,
                                     final long reverseWindowScaleUnits,
                                     final long forwardWindowScaleUnits,
                                     final boolean isMax
                                     // region extra-constructor-args
                                     // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }
}

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteRollingMinMaxOperator extends BaseByteUpdateByOperator {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        protected ByteChunk<? extends Values> byteInfluencerValuesChunk;
        protected AggregatingByteRingBuffer aggMinMax;
        protected boolean evaluationNeeded;

        protected Context(final int chunkSize) {
            super(chunkSize);
            if (isMax) {
                aggMinMax = new AggregatingByteRingBuffer(BUFFER_INITIAL_CAPACITY, Byte.MIN_VALUE, (a, b) -> {
                    if (a == NULL_BYTE) {
                        return b;
                    } else if (b == NULL_BYTE) {
                        return a;
                    }
                    return (byte)Math.max(a, b);
                });
            } else {
                aggMinMax = new AggregatingByteRingBuffer(BUFFER_INITIAL_CAPACITY, Byte.MAX_VALUE, (a, b) -> {
                    if (a == NULL_BYTE) {
                        return b;
                    } else if (b == NULL_BYTE) {
                        return a;
                    }
                    return (byte)Math.min(a, b);
                });
            }
            curVal = isMax ? Byte.MIN_VALUE : Byte.MAX_VALUE;
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            byteInfluencerValuesChunk = valuesChunk.asByteChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                byte val = byteInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == NULL_BYTE) {
                    nullCount++;
                } else if (curVal == NULL_BYTE) {
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
            Assert.geq(aggMinMax.size(), "byteWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                byte val = aggMinMax.removeUnsafe();

                if (val == NULL_BYTE) {
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
                curVal = NULL_BYTE;
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
            curVal = isMax ? Byte.MIN_VALUE : Byte.MAX_VALUE;
            evaluationNeeded = false;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public ByteRollingMinMaxOperator(@NotNull final MatchPair pair,
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

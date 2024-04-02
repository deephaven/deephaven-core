//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingminmax;

import io.deephaven.base.ringbuffer.AggregatingObjectRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ComparableRollingMinMaxOperator<T extends Comparable<T>> extends BaseObjectUpdateByOperator<T> {
    private final boolean isMax;
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        protected ObjectChunk<T, ? extends Values> objectInfluencerValuesChunk;
        protected AggregatingObjectRingBuffer<T> aggMinMax;
        protected boolean evaluationNeeded;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            if (isMax) {
                aggMinMax = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, null, (a, b) -> {
                    if (a == null) {
                        return b;
                    } else if (b == null) {
                        return a;
                    }
                    return a.compareTo(b) > 0 ? a : b;
                });
            } else {
                aggMinMax = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_CAPACITY, null, (a, b) -> {
                    if (a == null) {
                        return b;
                    } else if (b == null) {
                        return a;
                    }
                    return a.compareTo(b) < 0 ? a : b;
                });
            }
            evaluationNeeded = false;
        }

        @Override
        public void close() {
            super.close();
            aggMinMax = null;
        }


        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectInfluencerValuesChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            aggMinMax.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                T val = objectInfluencerValuesChunk.get(pos + ii);
                aggMinMax.addUnsafe(val);

                if (val == null) {
                    nullCount++;
                } else if (curVal == null) {
                    curVal = val;
                } else if (isMax && curVal.compareTo(val) < 0) {
                    curVal = val;
                    evaluationNeeded = false;
                } else if (!isMax && curVal.compareTo(val) > 0) {
                    curVal = val;
                    evaluationNeeded = false;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(aggMinMax.size(), "aggMinMax.size()", count);

            for (int ii = 0; ii < count; ii++) {
                T val = aggMinMax.removeUnsafe();

                if (val == null) {
                    nullCount--;
                } else {
                    // Only revaluate if we pop something equal to our current value. Otherwise we have perfect
                    // confidence that the min/max is still in the window.
                    if (curVal != null && curVal.compareTo(val) == 0) {
                        evaluationNeeded = true;
                    }
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (aggMinMax.size() == nullCount) {
                curVal = null;
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
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public ComparableRollingMinMaxOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final boolean isMax,
            final Class<?> colType) {
        // noinspection unchecked
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true,
                (Class<T>) colType);
        this.isMax = isMax;
    }

    @Override
    public UpdateByOperator copy() {
        return new ComparableRollingMinMaxOperator<>(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                isMax,
                colType);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.base.RingBuffer;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerRollingSumOperator extends BaseObjectUpdateByOperator<BigInteger> {

    private static final int RING_BUFFER_INITIAL_CAPACITY = 512;

    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> objectInfluencerValuesChunk;
        protected RingBuffer<BigInteger> objectWindowValues;

        protected Context(final int chunkSize) {
            super(chunkSize);
            objectWindowValues = new RingBuffer<>(RING_BUFFER_INITIAL_CAPACITY);
        }

        @Override
        public void close() {
            super.close();
            objectWindowValues = null;
        }


        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectInfluencerValuesChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectInfluencerValuesChunk.get(pos + ii);
                objectWindowValues.add(val);

                // increase the running sum
                if (val != null) {
                    if (curVal == null) {
                        curVal = val;
                    } else {
                        curVal = curVal.add(val);
                    }
                } else {
                    nullCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectWindowValues.remove();

                // reduce the running sum
                if (val != null) {
                    curVal = curVal.subtract(val);
                } else {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (objectWindowValues.size() == nullCount) {
                outputValues.set(outIdx, null);
                curVal = null;
            } else {
                outputValues.set(outIdx, curVal);
            }
        }

        @Override
        public void reset() {
            super.reset();
            objectWindowValues.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    public BigIntegerRollingSumOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true,
                BigInteger.class);
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerRollingSumOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits);
    }
}

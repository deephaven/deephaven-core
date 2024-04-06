//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingproduct;

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

import java.math.BigInteger;

public final class BigIntegerRollingProductOperator extends BaseObjectUpdateByOperator<BigInteger> {

    private static final int BUFFER_INITIAL_SIZE = 64;

    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        protected ObjectChunk<BigInteger, ? extends Values> objectInfluencerValuesChunk;
        protected AggregatingObjectRingBuffer<BigInteger> buffer;

        private int zeroCount;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            buffer = new AggregatingObjectRingBuffer<>(BUFFER_INITIAL_SIZE,
                    BigInteger.ONE,
                    BigInteger::multiply, // tree function
                    (a, b) -> { // value function
                        if (a == null && b == null) {
                            return BigInteger.ONE; // identity val
                        } else if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        }
                        return a.multiply(b);
                    },
                    true);
            zeroCount = 0;
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
        }


        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectInfluencerValuesChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                BigInteger val = objectInfluencerValuesChunk.get(pos + ii);
                buffer.addUnsafe(val);

                if (val == null) {
                    nullCount++;
                } else if (val.equals(BigInteger.ZERO)) {
                    zeroCount++;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(buffer.size(), "buffer.size()", count);

            for (int ii = 0; ii < count; ii++) {
                BigInteger val = buffer.removeUnsafe();

                if (val == null) {
                    nullCount--;
                } else if (val.equals(BigInteger.ZERO)) {
                    --zeroCount;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            if (buffer.size() == nullCount) {
                outputValues.set(outIdx, null);
            } else {
                outputValues.set(outIdx, zeroCount > 0 ? BigInteger.ZERO : buffer.evaluate());
            }
        }

        @Override
        public void reset() {
            super.reset();
            zeroCount = 0;
            buffer.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public BigIntegerRollingProductOperator(
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
        return new BigIntegerRollingProductOperator(
                pair,
                affectingColumns,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits);
    }
}

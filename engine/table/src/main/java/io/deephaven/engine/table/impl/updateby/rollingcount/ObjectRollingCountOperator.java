//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.rollingcount;

import io.deephaven.base.ringbuffer.ByteRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ObjectRollingCountOperator extends BaseLongUpdateByOperator {
    private static final int BUFFER_INITIAL_CAPACITY = 128;

    protected class Context extends BaseLongUpdateByOperator.Context {
        protected ObjectChunk<Object, ? extends Values> influencerValuesChunk;
        protected ByteRingBuffer buffer;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
            buffer = new ByteRingBuffer(BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            influencerValuesChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            buffer.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final Object val = influencerValuesChunk.get(pos + ii);

                if (val == null) {
                    buffer.addUnsafe((byte) 0); // 0 signifies null
                    nullCount++;
                } else {
                    buffer.addUnsafe((byte) 1); // 1 signifies non-null
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(buffer.size(), "charWindowValues.size()", count);

            for (int ii = 0; ii < count; ii++) {
                final byte val = buffer.removeUnsafe();

                if (val == 0) {
                    nullCount--;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            curVal = buffer.size() - nullCount;
            outputValues.set(outIdx, curVal);
        }

        @Override
        public void reset() {
            super.reset();
            buffer.clear();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public ObjectRollingCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, true);
    }

    @Override
    public UpdateByOperator copy() {
        return new ObjectRollingCountOperator(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits);
    }
}

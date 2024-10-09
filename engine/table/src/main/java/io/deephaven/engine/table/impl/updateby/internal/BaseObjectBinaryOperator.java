//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        public ObjectChunk<T, ? extends Values> objectValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectValueChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            for (int ii = 0; ii < count; ii++) {
                // read the value from the values chunk
                final T currentVal = objectValueChunk.get(pos + ii);
                if (curVal == null) {
                    curVal = currentVal;
                } else if (currentVal != null) {
                    curVal = doOperation(curVal, currentVal);
                }
            }
        }

        @Override
        public void reset() {
            curVal = null;
        }
    }

    public BaseObjectBinaryOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final Class<T> type) {
        super(pair, affectingColumns, type);
    }

    protected abstract T doOperation(T bucketCurVal, T chunkCurVal);

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}

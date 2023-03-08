package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        public ObjectChunk<T, ? extends Values> objectValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
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
                                    @Nullable final RowRedirection rowRedirection,
                                    @NotNull final Class<T> type) {
        super(pair, affectingColumns, rowRedirection, type);
    }

    protected abstract T doOperation(T bucketCurVal, T chunkCurVal);

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}

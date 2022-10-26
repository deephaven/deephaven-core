package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        public ObjectChunk<T, ? extends Values> objectValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final T currentVal = objectValueChunk.get(pos);
            if(curVal == null) {
                curVal = currentVal;
            } else if(currentVal != null) {
                curVal = doOperation(curVal, currentVal);
            }
        }

        @Override
        public void reset() {
            curVal = null;
        }
    }

    public BaseObjectBinaryOperator(@NotNull final Class<T> type,
                                    @NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, redirContext, type);
    }

    protected abstract T doOperation(T bucketCurVal, T chunkCurVal);

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }
}

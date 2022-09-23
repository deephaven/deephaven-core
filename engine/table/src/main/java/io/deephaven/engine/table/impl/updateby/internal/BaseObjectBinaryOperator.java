package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class BaseObjectBinaryOperator<T> extends BaseObjectUpdateByOperator<T> {
    protected class Context extends BaseObjectUpdateByOperator<T>.Context {
        public ObjectChunk<T, Values> objectValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
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

    // region Processing
    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @Nullable final LongChunk<OrderedRowKeys> posChunk,
                             @Nullable final Chunk<Values> valuesChunk,
                             @Nullable final LongChunk<Values> timestampValuesChunk) {
        Assert.neqNull(valuesChunk, "valuesChunk must not be null for a cumulative operator");
        final Context ctx = (Context) updateContext;
        ctx.storeValuesChunk(valuesChunk);
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.set(ii, ctx.curVal);
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues, inputKeys);
    }
    // endregion

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        // read the value from the values chunk
        final T currentVal = ctx.objectValueChunk.get(pos);
        if(ctx.curVal == null) {
            ctx.curVal = currentVal;
        } else if(currentVal != null) {
            ctx.curVal = doOperation(ctx.curVal, currentVal);
        }
    }
}

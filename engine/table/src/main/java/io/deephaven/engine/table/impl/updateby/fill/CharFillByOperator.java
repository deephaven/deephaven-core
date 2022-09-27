package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseCharUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class CharFillByOperator extends BaseCharUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseCharUpdateByOperator.Context {
        public CharChunk<Values> charValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            charValueChunk = valuesChunk.asCharChunk();
        }
    }

    public CharFillByOperator(@NotNull final MatchPair fillPair,
                              @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        char currentVal = ctx.charValueChunk.get(pos);
        if(currentVal != NULL_CHAR) {
            ctx.curVal = currentVal;
        }
    }

    // region extra-methods
    // endregion extra-methods
}

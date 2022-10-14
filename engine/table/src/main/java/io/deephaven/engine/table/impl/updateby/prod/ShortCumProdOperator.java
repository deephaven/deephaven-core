package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class ShortCumProdOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public ShortChunk<? extends Values> shortValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            shortValueChunk = valuesChunk.asShortChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final short currentVal = shortValueChunk.get(pos);

            if (curVal == NULL_LONG) {
                curVal = currentVal == NULL_SHORT ? NULL_LONG : currentVal;
            } else if (currentVal != NULL_SHORT) {
                curVal *= currentVal;
            }
        }

        @Override
        public void reset() {
            curVal = NULL_SHORT;
        }
    }

    public ShortCumProdOperator(@NotNull final MatchPair pair,
                               @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                               // region extra-constructor-args
                               // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, redirContext);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }
}

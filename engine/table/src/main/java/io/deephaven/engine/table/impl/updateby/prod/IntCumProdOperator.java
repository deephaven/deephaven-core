/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumProdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_INT;

public class IntCumProdOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public IntChunk<? extends Values> intValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            intValueChunk = valuesChunk.asIntChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            Assert.eq(count, "push count", 1);

            final int val = intValueChunk.get(pos);

            if (val != NULL_INT) {
                curVal = curVal == NULL_LONG ? val : curVal * val;
            }
        }
    }

    public IntCumProdOperator(@NotNull final MatchPair pair,
                               @Nullable final RowRedirection rowRedirection
                               // region extra-constructor-args
                               // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}

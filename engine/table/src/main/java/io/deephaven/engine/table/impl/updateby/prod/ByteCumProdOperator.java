/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumProdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteCumProdOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    final byte nullValue;
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public ByteChunk<? extends Values> byteValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            byteValueChunk = valueChunks[0].asByteChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            final byte val = byteValueChunk.get(pos);

            if (val != nullValue) {
                curVal = curVal == NULL_LONG ? val : curVal * val;
            }
        }
    }

    public ByteCumProdOperator(@NotNull final MatchPair pair,
                               @Nullable final RowRedirection rowRedirection
                               // region extra-constructor-args
                               ,final byte nullValue
                               // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
        // region constructor
        this.nullValue = nullValue;
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}

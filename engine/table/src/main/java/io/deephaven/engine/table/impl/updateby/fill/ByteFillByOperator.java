/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteFillByOperator extends BaseByteUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
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

            byte val = byteValueChunk.get(pos);
            if(val != NULL_BYTE) {
                curVal = val;
            }
        }
    }

    public ByteFillByOperator(
            @NotNull final MatchPair pair
            // region extra-constructor-args
            // endregion extra-constructor-args
            ) {
        super(pair, new String[] { pair.rightColumn });
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new ByteFillByOperator(
                pair
                // region extra-copy-args
                // endregion extra-copy-args
            );
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

    // region extra-methods
    // endregion extra-methods
}

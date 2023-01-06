/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFillByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.fill;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteFillByOperator extends BaseByteUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        public ByteChunk<? extends Values> byteValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            byteValueChunk = valuesChunk.asByteChunk();
        }

        @Override
        public void push(long key, int pos) {
            byte currentVal = byteValueChunk.get(pos);
            if(currentVal != NULL_BYTE) {
                curVal = currentVal;
            }
        }
    }

    public ByteFillByOperator(@NotNull final MatchPair fillPair,
                              @Nullable final RowRedirection rowRedirection
                              // region extra-constructor-args
                              // endregion extra-constructor-args
                              ) {
        super(fillPair, new String[] { fillPair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }

    // region extra-methods
    // endregion extra-methods
}

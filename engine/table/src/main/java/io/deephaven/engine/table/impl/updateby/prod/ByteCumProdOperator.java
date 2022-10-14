/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumProdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class ByteCumProdOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
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
            // read the value from the values chunk
            final byte currentVal = byteValueChunk.get(pos);

            if (curVal == NULL_LONG) {
                curVal = currentVal == NULL_BYTE ? NULL_LONG : currentVal;
            } else if (currentVal != NULL_BYTE) {
                curVal *= currentVal;
            }
        }

        @Override
        public void reset() {
            curVal = NULL_BYTE;
        }
    }

    public ByteCumProdOperator(@NotNull final MatchPair pair,
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

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class ByteCumMinMaxOperator extends BaseByteUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        public ByteChunk<Values> byteValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            byteValueChunk = valuesChunk.asByteChunk();
        }
    }

    public ByteCumMinMaxOperator(@NotNull final MatchPair pair,
                                  final boolean isMax,
                                  @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, redirContext);
        this.isMax = isMax;
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

        // read the value from the values chunk
        final byte currentVal = ctx.byteValueChunk.get(pos);

        if (ctx.curVal == NULL_BYTE) {
            ctx.curVal = currentVal;
        } else if (currentVal != NULL_BYTE) {
            if ((isMax && currentVal > ctx.curVal) ||
                    (!isMax && currentVal < ctx.curVal)) {
                ctx.curVal = currentVal;
            }
        }
    }
    // region extra-methods
    // endregion extra-methods
}

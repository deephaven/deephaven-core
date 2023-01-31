/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortCumMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseByteUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ByteCumMinMaxOperator extends BaseByteUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseByteUpdateByOperator.Context {
        public ByteChunk<? extends Values> byteValueChunk;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            byteValueChunk = valuesChunk.asByteChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
            Assert.eq(count, "push count", 1);

            final byte val = byteValueChunk.get(pos);

            if (curVal == NULL_BYTE) {
                curVal = val;
            } else if (val != NULL_BYTE) {
                if ((isMax && val > curVal) ||
                        (!isMax && val < curVal)) {
                    curVal = val;
                }
            }
        }
    }

    public ByteCumMinMaxOperator(@NotNull final MatchPair pair,
                                  final boolean isMax,
                                  @Nullable final RowRedirection rowRedirection
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection, false);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }
    // region extra-methods
    // endregion extra-methods

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }
}

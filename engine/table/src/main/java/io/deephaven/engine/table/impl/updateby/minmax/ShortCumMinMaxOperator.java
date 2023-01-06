package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseShortUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortCumMinMaxOperator extends BaseShortUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseShortUpdateByOperator.Context {
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

            if (curVal == NULL_SHORT) {
                curVal = currentVal;
            } else if (currentVal != NULL_SHORT) {
                if ((isMax && currentVal > curVal) ||
                        (!isMax && currentVal < curVal)) {
                    curVal = currentVal;
                }
            }
        }
    }

    public ShortCumMinMaxOperator(@NotNull final MatchPair pair,
                                  final boolean isMax,
                                  @Nullable final RowRedirection rowRedirection
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }
}

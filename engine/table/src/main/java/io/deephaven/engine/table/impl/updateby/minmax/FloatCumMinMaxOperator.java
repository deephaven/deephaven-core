package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCumMinMaxOperator extends BaseFloatUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseFloatUpdateByOperator.Context {
        public FloatChunk<? extends Values> floatValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            floatValueChunk = valuesChunk.asFloatChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final float currentVal = floatValueChunk.get(pos);

            if(curVal == NULL_FLOAT) {
                curVal = currentVal;
            } else if(currentVal != NULL_FLOAT) {
                if ((isMax && currentVal > curVal) ||
                        (!isMax && currentVal < curVal)) {
                    curVal = currentVal;
                }
            }
        }

        @Override
        public void reset() {
            curVal = NULL_FLOAT;
        }
    }

    public FloatCumMinMaxOperator(@NotNull final MatchPair pair,
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
}

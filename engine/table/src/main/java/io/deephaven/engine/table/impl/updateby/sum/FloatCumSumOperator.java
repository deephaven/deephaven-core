package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCumSumOperator extends BaseFloatUpdateByOperator {
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

            if (curVal == NULL_FLOAT) {
                curVal = currentVal;
            } else if (currentVal != NULL_FLOAT) {
                curVal += currentVal;
            }
        }
    }

    public FloatCumSumOperator(@NotNull final MatchPair pair,
                               @Nullable final WritableRowRedirection rowRedirection
                               // region extra-constructor-args
                               // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }
}

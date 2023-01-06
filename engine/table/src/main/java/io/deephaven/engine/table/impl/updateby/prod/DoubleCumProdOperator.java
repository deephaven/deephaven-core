/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatCumProdOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleCumProdOperator extends BaseDoubleUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        public DoubleChunk<? extends Values> doubleValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleValueChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final double currentVal = doubleValueChunk.get(pos);

            if (curVal == NULL_DOUBLE) {
                curVal = currentVal;
            } else if (currentVal != NULL_DOUBLE) {
                curVal *= currentVal;
            }
        }
    }

    public DoubleCumProdOperator(@NotNull final MatchPair pair,
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
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }


}

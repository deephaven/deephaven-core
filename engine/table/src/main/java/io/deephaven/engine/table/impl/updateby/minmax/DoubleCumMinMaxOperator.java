/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatCumMinMaxOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleCumMinMaxOperator extends BaseDoubleUpdateByOperator {
    private final boolean isMax;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        public DoubleChunk<Values> doubleValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            doubleValueChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final double currentVal = doubleValueChunk.get(pos);

            if(curVal == NULL_DOUBLE) {
                curVal = currentVal;
            } else if(currentVal != NULL_DOUBLE) {
                if ((isMax && currentVal > curVal) ||
                        (!isMax && currentVal < curVal)) {
                    curVal = currentVal;
                }
            }
        }

        @Override
        public void reset() {
            curVal = NULL_DOUBLE;
        }
    }

    public DoubleCumMinMaxOperator(@NotNull final MatchPair pair,
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

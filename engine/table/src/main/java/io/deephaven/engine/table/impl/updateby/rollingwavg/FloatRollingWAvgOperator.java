/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollingWAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class FloatRollingWAvgOperator extends BasePrimitiveRollingWAvgOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BasePrimitiveRollingWAvgOperator.Context {
        protected FloatChunk<? extends Values> influencerValuesChunk;

        protected Context(int affectedChunkSize, int influencerChunkSize) {
            super(affectedChunkSize, influencerChunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            super.setValueChunks(valueChunks);
            influencerValuesChunk = valueChunks[0].asFloatChunk();
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);
            windowWeightValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final float val = influencerValuesChunk.get(pos + ii);
                final double weight = influencerWeightValuesChunk.get(pos + ii);

                if (val == NULL_FLOAT || weight == NULL_DOUBLE) {
                    windowValues.addUnsafe(NULL_DOUBLE);
                    windowWeightValues.addUnsafe(NULL_DOUBLE);
                    nullCount++;
                } else {
                    // Compute the product and add to the agg buffer.
                    final double weightedVal = weight * val;
                    windowValues.addUnsafe(weightedVal);
                    windowWeightValues.addUnsafe(weight);
                }
            }
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }

    public FloatRollingWAvgOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @Nullable final RowRedirection rowRedirection,
                                   @Nullable final String timestampColumnName,
                                   final long reverseWindowScaleUnits,
                                   final long forwardWindowScaleUnits,
                                   @NotNull final String weightColumnName,
                                   @NotNull final ChunkType weightChunkType,
                                   @NotNull final Class<?> weightColumnSourceType
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits, forwardWindowScaleUnits, weightColumnName, weightChunkType, weightColumnSourceType);
        // region constructor
        // endregion constructor
    }

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    @Override
    protected String[] getInputColumnNames() {
        return new String[] {pair.rightColumn, weightColumnName};
    }
}

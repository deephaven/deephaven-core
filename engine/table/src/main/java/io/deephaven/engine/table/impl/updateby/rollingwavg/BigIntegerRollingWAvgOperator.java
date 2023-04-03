package io.deephaven.engine.table.impl.updateby.rollingwavg;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public final class BigIntegerRollingWAvgOperator extends BaseBigNumberRollingWAvgOperator<BigInteger> {
    protected class Context extends BaseBigNumberRollingWAvgOperator<BigInteger>.Context {

        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize, influencerChunkSize);
        }

        @Override
        public void push(int pos, int count) {
            windowValues.ensureRemaining(count);
            windowWeightValues.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                final BigInteger val = influencerValuesChunk.get(pos + ii);
                final BigDecimal weight = influencerWeightValuesChunk.get(pos + ii);

                if (val == null || weight == null) {
                    windowValues.addUnsafe(null);
                    windowWeightValues.addUnsafe(null);
                    nullCount++;
                } else {
                    // Compute the product and add to the agg buffer.
                    final BigDecimal decVal = new BigDecimal(val);
                    final BigDecimal weightedVal = weight.multiply(decVal, mathContext);
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

    public BigIntegerRollingWAvgOperator(@NotNull final MatchPair pair,
                                         @NotNull final String[] affectingColumns,
                                         @Nullable final RowRedirection rowRedirection,
                                         @Nullable final String timestampColumnName,
                                         final long reverseWindowScaleUnits,
                                         final long forwardWindowScaleUnits,
                                         @NotNull final String weightColumnName,
                                         @NotNull final ChunkType weightChunkType,
                                         @NotNull final Class<?> weightColumnSourceType,
                                         @NotNull final MathContext mathContext
                                         // region extra-constructor-args
                                         // endregion extra-constructor-args
                                        ) {
        super(pair,
                affectingColumns,
                rowRedirection,
                timestampColumnName,
                reverseWindowScaleUnits,
                forwardWindowScaleUnits,
                weightColumnName,
                weightChunkType,
                weightColumnSourceType,
                mathContext);
        // region constructor
        // endregion constructor        
    }
}

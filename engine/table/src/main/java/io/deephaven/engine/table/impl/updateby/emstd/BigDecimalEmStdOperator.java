package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class BigDecimalEmStdOperator extends BaseBigNumberEmStdOperator<BigDecimal> {
    public class Context extends BaseBigNumberEmStdOperator<BigDecimal>.Context {
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
        }

        @Override
        public void accumulateCumulative(@NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @NotNull final LongChunk<? extends Values> tsChunk,
                final int len) {
            setValueChunks(valueChunkArr);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigDecimal input = objectValueChunk.get(ii);
                    if (input == null) {
                        handleBadData(this, true);
                    } else {
                        if (curEma == null) {
                            curEma = input;
                            curVariance = BigDecimal.ZERO;
                            curVal = null;
                        } else {
                            // incremental variance = alpha * (prevVariance + (1 − alpha) * (x − prevEma)^2)
                            curVariance = opAlpha.multiply(
                                    curVariance.add(
                                            opOneMinusAlpha.multiply(input.subtract(curEma).pow(2, mathContext)),
                                            mathContext),
                                    mathContext);

                            final BigDecimal decayedEmaVal = curEma.multiply(opAlpha, mathContext);
                            curEma = decayedEmaVal.add(opOneMinusAlpha.multiply(input, mathContext));
                            curVal = curVariance.sqrt(mathContext);
                        }
                    }
                    outputValues.set(ii, curVal);
                    if (emaValues != null) {
                        emaValues.set(ii, curEma);
                    }
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigDecimal input = objectValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == null;
                    final boolean isNullTime = timestamp == NULL_LONG;
                    if (isNull) {
                        handleBadData(this, isNull);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else {
                        if (curEma == null) {
                            curEma = input;
                            lastStamp = timestamp;
                        } else {
                            final long dt = timestamp - lastStamp;
                            if (dt != 0) {
                                // alpha is dynamic based on time, but only recalculated when needed
                                if (dt != lastDt) {
                                    alpha = computeAlpha(-dt, reverseWindowScaleUnits);
                                    oneMinusAlpha = computeOneMinusAlpha(alpha);
                                    lastDt = dt;
                                }
                                // incremental variance = alpha * (prevVariance + (1 − alpha) * (x − prevEma)^2)
                                curVariance = alpha.multiply(
                                        curVariance.add(
                                                oneMinusAlpha.multiply(input.subtract(curEma).pow(2, mathContext)),
                                                mathContext),
                                        mathContext);

                                final BigDecimal decayedEmaVal = curEma.multiply(alpha, mathContext);
                                curEma = decayedEmaVal.add(oneMinusAlpha.multiply(input, mathContext));
                                curVal = curVariance.sqrt(mathContext);
                                lastStamp = timestamp;
                            }
                        }
                    }
                    outputValues.set(ii, curVal);
                    if (emaValues != null) {
                        emaValues.set(ii, curEma);
                    }
                }
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void push(int pos, int count) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }

    /**
     * An operator that computes an EMA from a BigDecimal column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param rowRedirection the {@link RowRedirection} to use for dense output sources
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     * @param valueSource a reference to the input column source for this operation
     */
    public BigDecimalEmStdOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            final ColumnSource<?> valueSource,
            final boolean sourceRefreshing,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, rowRedirection, control, timestampColumnName, windowScaleUnits, valueSource,
                sourceRefreshing, mathContext);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}

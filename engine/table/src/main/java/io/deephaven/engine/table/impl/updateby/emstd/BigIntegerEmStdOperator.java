//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/***
 * Compute an exponential moving standard deviation for a BigInteger column source. The output is expressed as a
 * BigDecimal value and is computed using the following formula:
 * <p>
 * variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
 * <p>
 * This function is described in the following document:
 * <p>
 * "Incremental calculation of weighted mean and variance" Tony Finch, University of Cambridge Computing Service
 * (February 2009)
 * https://web.archive.org/web/20181222175223/http://people.ds.cam.ac.uk/fanf2/hermes/doc/antiforgery/stats.pdf
 * <p>
 * NOTE: `alpha` as used in the paper has been replaced with `1 - alpha` per the convention adopted by Deephaven.
 */
public class BigIntegerEmStdOperator extends BaseBigNumberEmStdOperator<BigInteger> {
    public class Context extends BaseBigNumberEmStdOperator<BigInteger>.Context {
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
        }

        @Override
        public void accumulateCumulative(@NotNull final RowSequence inputKeys,
                @NotNull final Chunk<? extends Values>[] valueChunkArr,
                @Nullable final LongChunk<? extends Values> tsChunk,
                final int len) {
            setValueChunks(valueChunkArr);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final BigInteger input = objectValueChunk.get(ii);
                    if (input == null) {
                        handleBadData(this, true);
                    } else {
                        final BigDecimal decInput = new BigDecimal(input);
                        if (curEma == null) {
                            curEma = decInput;
                            curVariance = BigDecimal.ZERO;
                            curVal = null;
                        } else {
                            // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                            curVariance = opAlpha.multiply(
                                    curVariance.add(
                                            opOneMinusAlpha.multiply(decInput.subtract(curEma).pow(2, mathContext)),
                                            mathContext),
                                    mathContext);

                            final BigDecimal decayedEmaVal = curEma.multiply(opAlpha, mathContext);
                            curEma = decayedEmaVal.add(opOneMinusAlpha.multiply(decInput, mathContext));
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
                    final BigInteger input = objectValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    final boolean isNull = input == null;
                    final boolean isNullTime = timestamp == NULL_LONG;
                    if (isNull) {
                        handleBadData(this, isNull);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else if (curEma == null) {
                        // We have a valid input value, we can initialize the output value with it.
                        curEma = new BigDecimal(input, mathContext);
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt < 0) {
                            // negative time deltas are not allowed, throw an exception
                            throw new TableDataException("Timestamp values in UpdateBy operators must not decrease");
                        }
                        if (dt != 0) {
                            // alpha is dynamic based on time, but only recalculated when needed
                            if (dt != lastDt) {
                                alpha = computeAlpha(-dt, reverseWindowScaleUnits);
                                oneMinusAlpha = computeOneMinusAlpha(alpha);
                                lastDt = dt;
                            }
                            // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                            final BigDecimal decInput = new BigDecimal(input, mathContext);
                            curVariance = alpha.multiply(
                                    curVariance.add(
                                            oneMinusAlpha.multiply(decInput.subtract(curEma).pow(2, mathContext)),
                                            mathContext),
                                    mathContext);

                            final BigDecimal decayedEmaVal = curEma.multiply(alpha, mathContext);
                            curEma = decayedEmaVal.add(oneMinusAlpha.multiply(decInput, mathContext));
                            curVal = curVariance.sqrt(mathContext);
                            lastStamp = timestamp;
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
    }

    /**
     * An operator that computes an EM Std from a BigDecimal column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     */
    public BigIntegerEmStdOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, control, timestampColumnName, windowScaleUnits, mathContext);
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerEmStdOperator(
                pair,
                affectingColumns,
                control,
                timestampColumnName,
                reverseWindowScaleUnits,
                mathContext);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}

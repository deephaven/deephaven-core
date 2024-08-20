//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.emstd;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

/***
 * Compute an exponential moving standard deviation for a char column source. The output is expressed as a double value
 * and is computed using the following formula:
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
public class CharEmStdOperator extends BasePrimitiveEmStdOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BasePrimitiveEmStdOperator.Context {

        public CharChunk<? extends Values> charValueChunk;

        @SuppressWarnings("unused")
        protected Context(final int affectedChunkSize, final int influencerChunkSize) {
            super(affectedChunkSize);
        }

        @Override
        public void accumulateCumulative(@NotNull RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len) {
            setValueChunks(valueChunkArr);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final char input = charValueChunk.get(ii);

                    if (input == NULL_CHAR) {
                        handleBadData(this, true, false);
                    } else {
                        if (curEma == NULL_DOUBLE) {
                            curEma = input;
                            curVariance = 0.0;
                            curVal = Double.NaN;
                        } else {
                            // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                            curVariance = opAlpha * (curVariance + opOneMinusAlpha * Math.pow(input - curEma, 2.0));

                            final double decayedEmaVal = curEma * opAlpha;
                            curEma = decayedEmaVal + (opOneMinusAlpha * input);
                            curVal = Math.sqrt(curVariance);
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
                    final char input = charValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    // noinspection ConstantConditions
                    final boolean isNull = input == NULL_CHAR;
                    final boolean isNullTime = timestamp == NULL_LONG;

                    if (isNull) {
                        handleBadData(this, true, false);
                    } else if (isNullTime) {
                        // no change to curVal and lastStamp
                    } else if (curEma == NULL_DOUBLE) {
                        curEma = input;
                        curVariance = 0.0;
                        curVal = Double.NaN;
                        lastStamp = timestamp;
                    } else {
                        final long dt = timestamp - lastStamp;
                        if (dt < 0) {
                            // negative time deltas are not allowed, throw an exception
                            throw new TableDataException("Timestamp values in UpdateBy operators must not decrease");
                        }
                        if (dt != lastDt) {
                            // Alpha is dynamic based on time, but only recalculated when needed
                            alpha = Math.exp(-dt / reverseWindowScaleUnits);
                            oneMinusAlpha = 1.0 - alpha;
                            lastDt = dt;
                        }
                        // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                        curVariance = alpha * (curVariance + oneMinusAlpha * Math.pow(input - curEma, 2.0));

                        final double decayedEmaVal = curEma * alpha;
                        curEma = decayedEmaVal + (oneMinusAlpha * input);
                        curVal = Math.sqrt(curVariance);

                        lastStamp = timestamp;
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
        public void writeToOutputColumn(@NotNull final RowSequence inputKeys) {
            outputSource.fillFromChunk(outputFillContext, outputValues, inputKeys);
            if (emaValues != null) {
                emaSource.fillFromChunk(emaFillContext, emaValues, inputKeys);
            }
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valuesChunks) {
            charValueChunk = valuesChunks[0].asCharChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.getChar(atKey) != NULL_CHAR;
        }
    }

    /**
     * An operator that computes an exponential moving standard deviation from a char column using an exponential decay
     * function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timestampColumnName the name of the column containing timestamps for time-based calcuations
     * @param windowScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds
     */
    public CharEmStdOperator(
            @NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final double windowScaleUnits
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, windowScaleUnits);
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new CharEmStdOperator(
                pair,
                affectingColumns,
                control,
                timestampColumnName,
                reverseWindowScaleUnits
        // region extra-copy-args
        // endregion extra-copy-args
        );
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize, influencerChunkSize);
    }
}

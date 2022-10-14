package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

public class ShortEMAOperator extends BasePrimitiveEMAOperator {

    protected class Context extends BasePrimitiveEMAOperator.Context {
        public final ColumnSource<?> valueSource;

        public ShortChunk<? extends Values> shortValueChunk;

        protected Context(int chunkSize, ColumnSource<?> inputSource) {
            super(chunkSize);
            this.valueSource = inputSource;
        }

        @Override
        public void accumulate(RowSequence inputKeys,
                               Chunk<? extends Values> valueChunk,
                               LongChunk<? extends Values> tsChunk,
                               int len) {
            setValuesChunk(valueChunk);
            setTimestampChunk(tsChunk);

            // chunk processing
            if (timestampColumnName == null) {
                // compute with ticks
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final short input = shortValueChunk.get(ii);

                    if(input == NULL_SHORT) {
                        handleBadData(this, true, false, false);
                    } else {
                        if(curVal == NULL_DOUBLE) {
                            curVal = input;
                        } else {
                            curVal = alpha * curVal + (oneMinusAlpha * input);
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            } else {
                // compute with time
                for (int ii = 0; ii < len; ii++) {
                    // read the value from the values chunk
                    final short input = shortValueChunk.get(ii);
                    final long timestamp = tsChunk.get(ii);
                    //noinspection ConstantConditions
                    final boolean isNull = input == NULL_SHORT;
                    final boolean isNullTime = timestamp == NULL_LONG;
                    if(isNull || isNullTime) {
                        handleBadData(this, isNull, false, isNullTime);
                    } else {
                        if(curVal == NULL_DOUBLE) {
                            curVal = input;
                            lastStamp = timestamp;
                        } else {
                            final long dt = timestamp - lastStamp;
                            if(dt <= 0) {
                                handleBadTime(this, dt);
                            } else {
                                // alpha is dynamic, based on time
                                final double alpha = Math.exp(-dt / timeScaleUnits);
                                curVal = alpha * curVal + ((1 - alpha) * input);
                                lastStamp = timestamp;
                            }
                        }
                    }
                    outputValues.set(ii, curVal);
                }
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            shortValueChunk = valuesChunk.asShortChunk();
        }

        @Override
        public boolean isValueValid(long atKey) {
            return valueSource.getShort(atKey) != NULL_SHORT;
        }

        @Override
        public void push(long key, int pos) {
            throw new IllegalStateException("EMAOperator#push() is not used");
        }
    }

    /**
     * An operator that computes an EMA from a short column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control        defines how to handle {@code null} input values.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *                       in ticks, otherwise it is measured in nanoseconds
     */
    public ShortEMAOperator(@NotNull final MatchPair pair,
                            @NotNull final String[] affectingColumns,
                            @NotNull final OperationControl control,
                            @Nullable final String timestampColumnName,
                            final long timeScaleUnits,
                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                            // region extra-constructor-args
                            // endregion extra-constructor-args
                            ) {
        super(pair, affectingColumns, control, timestampColumnName, timeScaleUnits,redirContext);
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize, inputSource);
    }
}

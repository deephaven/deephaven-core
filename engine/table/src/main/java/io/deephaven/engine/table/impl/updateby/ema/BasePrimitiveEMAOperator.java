package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BasePrimitiveEMAOperator extends BaseDoubleUpdateByOperator {
    protected final OperationControl control;
    protected final double alpha;
    protected double oneMinusAlpha;

    public abstract class Context extends BaseDoubleUpdateByOperator.Context {
        public LongChunk<? extends Values> timestampValueChunk;

        long lastStamp = NULL_LONG;

        Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
        }

        @Override
        public void reset() {
            super.reset();
            lastStamp = NULL_LONG;
        }
    }

    /**
     * An operator that computes an EMA from an input column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control the control parameters for EMA
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds.
     * @param rowRedirection the row redirection to use for the EMA output columns
     */
    public BasePrimitiveEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long timeScaleUnits,
            @Nullable final RowRedirection rowRedirection) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, timeScaleUnits);
        this.control = control;

        alpha = Math.exp(-1.0 / (double) timeScaleUnits);
        oneMinusAlpha = 1 - alpha;

    }

    @Override
    public void initializeUpdate(@NotNull final UpdateContext updateContext,
            final long firstUnmodifiedKey,
            final long firstUnmodifiedTimestamp) {
        super.initializeUpdate(updateContext, firstUnmodifiedKey, firstUnmodifiedTimestamp);

        final Context ctx = (Context) updateContext;
        // rely on the caller to validate this is a valid timestamp (or NULL_LONG when appropriate)
        ctx.lastStamp = firstUnmodifiedTimestamp;
    }

    void handleBadData(@NotNull final Context ctx,
            final boolean isNull,
            final boolean isNan) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered null value during EMA processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        } else if (isNan) {
            if (control.onNanValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered NaN value during EMA processing");
            } else if (control.onNanValueOrDefault() == BadDataBehavior.POISON) {
                ctx.curVal = Double.NaN;
            } else {
                doReset = control.onNanValueOrDefault() == BadDataBehavior.RESET;
            }
        }

        if (doReset) {
            ctx.reset();
        }
    }
}

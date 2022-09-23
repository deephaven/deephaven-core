package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BasePrimitiveEMAOperator extends BaseDoubleUpdateByOperator {
    protected final OperationControl control;
    protected final String timestampColumnName;
    protected final double timeScaleUnits;
    protected final double alpha;
    protected double oneMinusAlpha;

    class Context extends BaseDoubleUpdateByOperator.Context {
        public LongChunk<Values> timestampValueChunk;

        long lastStamp = NULL_LONG;

        Context(final int chunkSize) {
            super(chunkSize);
        }
    }

    /**
     * An operator that computes an EMA from a short column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control the control parameters for EMA
     * @param timestampColumnName an optional timestamp column. If this is null, it will be assumed time is measured in
     *        integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timestampColumnName} is provided, this is
     *        measured in ticks, otherwise it is measured in nanoseconds.
     * @param redirContext the row redirection context to use for the EMA
     */
    public BasePrimitiveEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long timeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, redirContext);
        this.control = control;
        this.timestampColumnName = timestampColumnName;
        this.timeScaleUnits = (double) timeScaleUnits;

        alpha = Math.exp(-1.0 / (double) timeScaleUnits);
        oneMinusAlpha = 1 - alpha;

    }

    @Override
    public void initializeUpdate(@NotNull final UpdateContext updateContext,
            @NotNull final long firstUnmodifiedKey, long firstUnmodifiedTimestamp) {
        super.initializeUpdate(updateContext, firstUnmodifiedKey, firstUnmodifiedTimestamp);

        final Context ctx = (Context) updateContext;
        // If we set the last state to null, then we know it was a reset state and the timestamp must also
        // have been reset.
        if (ctx.curVal == NULL_DOUBLE || (firstUnmodifiedKey == NULL_ROW_KEY)) {
            ctx.lastStamp = NULL_LONG;
        } else {
            // rely on the caller to validate this is a valid timestamp (or NULL_LONG when appropriate)
            ctx.lastStamp = firstUnmodifiedTimestamp;
        }
    }

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
            @NotNull final RowSequence inputKeys,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @Nullable final LongChunk<OrderedRowKeys> posChunk,
            @Nullable final Chunk<Values> valuesChunk,
            @Nullable final LongChunk<Values> timestampValuesChunk) {
        Assert.notEquals(valuesChunk, "valuesChunk must not be null for a cumulative operator", null);
        final Context ctx = (Context) updateContext;
        ctx.storeValuesChunk(valuesChunk);
        ctx.timestampValueChunk = timestampValuesChunk;
        for (int ii = 0; ii < valuesChunk.size(); ii++) {
            push(ctx, keyChunk == null ? NULL_ROW_KEY : keyChunk.get(ii), ii);
            ctx.outputValues.set(ii, ctx.curVal);
        }
        outputSource.fillFromChunk(ctx.fillContext, ctx.outputValues, inputKeys);
    }

    void handleBadData(@NotNull final Context ctx,
            final boolean isNull,
            final boolean isNan,
            final boolean isNullTime) {
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

        if (isNullTime) {
            if (control.onNullTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered null timestamp during EMA processing");
            }
            doReset = control.onNullTimeOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.curVal = NULL_DOUBLE;
            ctx.lastStamp = NULL_LONG;
        }
    }

    void handleBadTime(@NotNull final Context ctx, final long dt) {
        boolean doReset = false;
        if (dt == 0) {
            if (control.onZeroDeltaTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered zero delta time during EMA processing");
            }
            doReset = control.onZeroDeltaTimeOrDefault() == BadDataBehavior.RESET;
        } else if (dt < 0) {
            if (control.onNegativeDeltaTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered negative delta time during EMA processing");
            }
            doReset = control.onNegativeDeltaTimeOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.curVal = NULL_DOUBLE;
            ctx.lastStamp = NULL_LONG;
        }
    }
}

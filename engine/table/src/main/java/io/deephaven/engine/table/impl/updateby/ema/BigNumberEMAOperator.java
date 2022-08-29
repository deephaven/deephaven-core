package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BigNumberEMAOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    protected final ColumnSource<T> valueSource;

    protected final OperationControl control;
    protected final LongRecordingUpdateByOperator timeRecorder;
    protected final String timestampColumnName;
    protected final double timeScaleUnits;

    class EmaContext extends Context {
        BigDecimal alpha = BigDecimal.valueOf(Math.exp(-1 / timeScaleUnits));
        BigDecimal oneMinusAlpha =
                timeRecorder == null ? BigDecimal.ONE.subtract(alpha, control.bigValueContextOrDefault()) : null;
        long lastStamp = NULL_LONG;

        EmaContext(final int chunkSize) {
            super(chunkSize);
        }
    }

    /**
     * An operator that computes an EMA from a int column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control defines how to handle {@code null} input values.
     * @param timeRecorder an optional recorder for a timestamp column. If this is null, it will be assumed time is
     *        measured in integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *        in ticks, otherwise it is measured in nanoseconds
     * @param valueSource the input column source. Used when determining reset positions for reprocessing
     */
    public BigNumberEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final LongRecordingUpdateByOperator timeRecorder,
            @Nullable final String timestampColumnName,
            final long timeScaleUnits,
            @NotNull ColumnSource<T> valueSource,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, redirContext, BigDecimal.class);
        this.control = control;
        this.timeRecorder = timeRecorder;
        this.timestampColumnName = timestampColumnName;
        this.timeScaleUnits = timeScaleUnits;
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new EmaContext(chunkSize);
    }

    @Override
    protected void doProcessChunk(@NotNull final Context updateContext,
            @NotNull final RowSequence inputKeys,
            @NotNull final Chunk<Values> workingChunk) {
        final ObjectChunk<T, Values> asObjects = workingChunk.asObjectChunk();
        final EmaContext ctx = (EmaContext) updateContext;

        if (timeRecorder == null) {
            computeWithTicks(ctx, asObjects, 0, inputKeys.intSize());
        } else {
            computeWithTime(ctx, asObjects, 0, inputKeys.intSize());
        }
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }


    @Override
    public void resetForProcess(@NotNull final UpdateContext context,
                                @NotNull final RowSet sourceIndex,
                                final long firstUnmodifiedKey) {
        super.resetForProcess(context, sourceIndex, firstUnmodifiedKey);

        if (timeRecorder == null) {
            return;
        }

        final EmaContext ctx = (EmaContext) context;

        // If we set the last state to null, then we know it was a reset state and the timestamp must also
        // have been reset.
        if (ctx.curVal == null || (firstUnmodifiedKey == NULL_ROW_KEY)) {
            ctx.lastStamp = NULL_LONG;
        } else {
            // If it hasn't been reset to null, then it's possible that the value at that position was null, in
            // which case  we must have ignored it, and so we have to actually keep looking backwards until we find
            // something not null.
            ctx.lastStamp = locateFirstValidPreviousTimestamp(sourceIndex, firstUnmodifiedKey);
        }
    }

    private long locateFirstValidPreviousTimestamp(@NotNull final RowSet indexToSearch,
            final long firstUnmodifiedKey) {
        long potentialResetTimestamp = timeRecorder.getCurrentLong(firstUnmodifiedKey);
        if (potentialResetTimestamp != NULL_LONG && isValueValid(firstUnmodifiedKey)) {
            return potentialResetTimestamp;
        }

        try (final RowSet.SearchIterator rIt = indexToSearch.reverseIterator()) {
            if (rIt.advance(firstUnmodifiedKey)) {
                while (rIt.hasNext()) {
                    final long nextKey = rIt.nextLong();
                    potentialResetTimestamp = timeRecorder.getCurrentLong(nextKey);
                    if (potentialResetTimestamp != NULL_LONG && isValueValid(nextKey)) {
                        return potentialResetTimestamp;
                    }
                }
            }
        }

        return NULL_LONG;
    }

    private boolean isValueValid(final long atKey) {
        return valueSource.get(atKey) != null;
    }

    abstract void computeWithTicks(final EmaContext ctx,
            final ObjectChunk<T, Values> valueChunk,
            final int chunkStart,
            final int chunkEnd);

    abstract void computeWithTime(final EmaContext ctx,
            final ObjectChunk<T, Values> valueChunk,
            final int chunkStart,
            final int chunkEnd);

    void handleBadData(@NotNull final EmaContext ctx,
            final boolean isNull,
            final boolean isNullTime) {
        boolean doReset = false;
        if (isNull) {
            if (control.onNullValueOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered invalid data during EMA processing");
            }
            doReset = control.onNullValueOrDefault() == BadDataBehavior.RESET;
        }

        if (isNullTime) {
            if (control.onNullTimeOrDefault() == BadDataBehavior.THROW) {
                throw new TableDataException("Encountered null timestamp during EMA processing");
            }
            doReset = control.onNullTimeOrDefault() == BadDataBehavior.RESET;
        }

        if (doReset) {
            ctx.curVal = null;
            ctx.lastStamp = NULL_LONG;
        }
    }

    void handleBadTime(@NotNull final EmaContext ctx, final long dt) {
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
            ctx.curVal = null;
            ctx.lastStamp = NULL_LONG;
        }
    }
}

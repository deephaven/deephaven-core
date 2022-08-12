package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BasePrimitiveEMAOperator extends BaseDoubleUpdateByOperator {
    protected final OperationControl control;
    protected final LongRecordingUpdateByOperator timeRecorder;
    protected final double timeScaleUnits;

    class EmaContext extends Context {
        double alpha;
        double oneMinusAlpha;
        long lastStamp = NULL_LONG;

        EmaContext(final double timeScaleUnits, final int chunkSize) {
            super(chunkSize);
            this.alpha = Math.exp(-1 / timeScaleUnits);
            this.oneMinusAlpha = 1 - alpha;
        }
    }

    /**
     * An operator that computes an EMA from a short column using an exponential decay function.
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this ema
     * @param control the control parameters for EMA
     * @param timeRecorder an optional recorder for a timestamp column. If this is null, it will be assumed time is
     *        measured in integer ticks.
     * @param timeScaleUnits the smoothing window for the EMA. If no {@code timeRecorder} is provided, this is measured
     *        in ticks, otherwise it is measured in nanoseconds.
     * @param rowRedirection the row redirection to use for the EMA
     */
    public BasePrimitiveEMAOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final LongRecordingUpdateByOperator timeRecorder,
            final long timeScaleUnits,
            @Nullable final RowRedirection rowRedirection) {
        super(pair, affectingColumns, rowRedirection);
        this.control = control;
        this.timeRecorder = timeRecorder;
        this.timeScaleUnits = timeScaleUnits;
    }

    @NotNull
    @Override
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize) {
        return new EmaContext(timeScaleUnits, chunkSize);
    }

    @Override
    public void initializeForUpdate(@NotNull final UpdateByOperator.UpdateContext context,
            @NotNull final TableUpdate upstream,
            @NotNull final RowSet resultSourceIndex,
            final long lastPrevKey ,
            final boolean isAppendOnly) {
        super.initializeForUpdate(context, upstream, resultSourceIndex, lastPrevKey, isAppendOnly);

        final EmaContext ctx = (EmaContext) context;
        // pre-load the context timestamp with the previous last value in the timestamp column (if possible)
        ctx.lastStamp = (lastPrevKey == NULL_ROW_KEY || timeRecorder == null) ? NULL_LONG : locateFirstValidPreviousTimestamp(resultSourceIndex, lastPrevKey);
    }

    @Override
    public void initializeFor(@NotNull final UpdateByOperator.UpdateContext updateContext,
            @NotNull final RowSet updateRowSet,
            @NotNull final UpdateBy.UpdateType type) {
        super.initializeFor(updateContext, updateRowSet, type);
    }

    @Override
    protected void doAddChunk(@NotNull final Context context,
            @NotNull final RowSequence inputKeys,
            @NotNull final Chunk<Values> workingChunk) {
        final EmaContext ctx = (EmaContext) context;
        if (timeRecorder == null) {
            computeWithTicks(ctx, workingChunk, 0, inputKeys.intSize());
        } else {
            computeWithTime(ctx, workingChunk, 0, inputKeys.intSize());
        }
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    @Override
    public void resetForReprocess(@NotNull final UpdateByOperator.UpdateContext context,
            @NotNull final RowSet sourceIndex,
            final long firstUnmodifiedKey) {
        super.resetForReprocess(context, sourceIndex, firstUnmodifiedKey);

        if (timeRecorder == null) {
            return;
        }

        final EmaContext ctx = (EmaContext) context;
        if (!ctx.canProcessDirectly) {
            // If we set the last state to null, then we know it was a reset state and the timestamp must also
            // have been reset.
            if (ctx.curVal == NULL_DOUBLE || (firstUnmodifiedKey == NULL_ROW_KEY)) {
                ctx.lastStamp = NULL_LONG;
            } else {
                // If it hasn't been reset to null, then it's possible that the value at that position was null, in
                // which case  we must have ignored it, and so we have to actually keep looking backwards until we find
                // something not null.
                ctx.lastStamp = locateFirstValidPreviousTimestamp(sourceIndex, firstUnmodifiedKey);
            }
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

    abstract boolean isValueValid(final long atKey);

    abstract void computeWithTicks(final EmaContext ctx,
            final Chunk<Values> valueChunk,
            final int chunkStart,
            final int chunkEnd);

    abstract void computeWithTime(final EmaContext ctx,
            final Chunk<Values> valueChunk,
            final int chunkStart,
            final int chunkEnd);

    void handleBadData(@NotNull final EmaContext ctx,
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
            ctx.curVal = NULL_DOUBLE;
            ctx.lastStamp = NULL_LONG;
        }
    }
}

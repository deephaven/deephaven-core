package io.deephaven.engine.table.impl.updateby.ema;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class BigNumberEMAOperator<T> extends BaseObjectUpdateByOperator<BigDecimal> {
    protected final ColumnSource<T> valueSource;

    protected final OperationControl control;
    protected final LongRecordingUpdateByOperator timeRecorder;
    protected final double timeScaleUnits;

    private LongArraySource bucketLastTimestamp;

    private long singletonLastStamp = NULL_LONG;
    private long singletonGroup = NULL_LONG;

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
            final long timeScaleUnits,
            @NotNull ColumnSource<T> valueSource,
            @Nullable final RowRedirection rowRedirection
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, BigDecimal.class);
        this.control = control;
        this.timeRecorder = timeRecorder;
        this.timeScaleUnits = timeScaleUnits;
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    @Override
    public void setBucketCapacity(final int capacity) {
        super.setBucketCapacity(capacity);
        bucketLastTimestamp.ensureCapacity(capacity);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize) {
        return new EmaContext(chunkSize);
    }

    @Override
    public void initializeForUpdate(@NotNull UpdateContext context,
            @NotNull TableUpdate upstream,
            @NotNull RowSet resultSourceIndex,
            boolean usingBuckets,
            boolean isAppendOnly) {
        // noinspection unchecked
        final EmaContext ctx = (EmaContext) context;
        if (!initialized) {
            initialized = true;
            if (usingBuckets) {
                this.bucketLastVal = new ObjectArraySource<>(BigDecimal.class);
                this.bucketLastTimestamp = new LongArraySource();
            }
        }

        // If we're redirected we have to make sure we tell the output source it's actual size, or we're going
        // to have a bad time. This is not necessary for non-redirections since the SparseArraySources do not
        // need to do anything with capacity.
        if (isRedirected) {
            outputSource.ensureCapacity(resultSourceIndex.size() + 1);
        }

        if (!usingBuckets) {
            // If we aren't bucketing, we'll just remember the appendyness.
            ctx.canProcessDirectly = isAppendOnly;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initializeFor(@NotNull final UpdateContext updateContext,
            @NotNull final RowSet updateIndex,
            @NotNull final UpdateBy.UpdateType type) {
        super.initializeFor(updateContext, updateIndex, type);
        ((EmaContext) updateContext).lastStamp = NULL_LONG;
    }

    @Override
    protected void doAddChunk(@NotNull final Context updateContext,
            @NotNull final RowSequence inputKeys,
            @NotNull final Chunk<Values> workingChunk,
            long groupPosition) {
        final ObjectChunk<T, Values> asObjects = workingChunk.asObjectChunk();
        final EmaContext ctx = (EmaContext) updateContext;

        if (groupPosition == singletonGroup) {
            ctx.lastStamp = singletonLastStamp;
            ctx.curVal = singletonVal;
        } else {
            ctx.lastStamp = NULL_LONG;
            ctx.curVal = null;
        }

        if (timeRecorder == null) {
            computeWithTicks(ctx, asObjects, 0, inputKeys.intSize());
        } else {
            computeWithTime(ctx, asObjects, 0, inputKeys.intSize());
        }

        singletonVal = ctx.curVal;
        singletonLastStamp = ctx.lastStamp;
        singletonGroup = groupPosition;
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    @Override
    public void addChunk(@NotNull final UpdateContext context,
            @NotNull final Chunk<Values> values,
            @NotNull final LongChunk<? extends RowKeys> keyChunk,
            @NotNull final IntChunk<RowKeys> bucketPositions,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> runLengths) {
        final ObjectChunk<T, Values> asObjects = values.asObjectChunk();
        // noinspection unchecked
        final EmaContext ctx = (EmaContext) context;
        for (int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int runEnd = runStart + runLength;
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.lastStamp = bucketLastTimestamp.getLong(bucketPosition);
            ctx.curVal = bucketLastVal.get(bucketPosition);
            if (timeRecorder == null) {
                computeWithTicks(ctx, asObjects, runStart, runEnd);
            } else {
                computeWithTime(ctx, asObjects, runStart, runEnd);
            }

            bucketLastVal.set(bucketPosition, ctx.curVal);
            bucketLastTimestamp.set(bucketPosition, ctx.lastStamp);
        }
        // noinspection unchecked
        outputSource.fillFromChunkUnordered(ctx.fillContext.get(), ctx.outputValues.get(),
                (LongChunk<RowKeys>) keyChunk);
    }

    @Override
    public void resetForReprocess(@NotNull final UpdateContext context,
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
            if (singletonVal == null || (firstUnmodifiedKey == NULL_ROW_KEY)) {
                singletonLastStamp = NULL_LONG;
            } else {
                // If it hasn't been reset to null, then it's possible that the value at that position was null, in
                // which case
                // we must have ignored it, and so we have to actually keep looking backwards until we find something
                // not null.


                // Note that it's OK that we are not setting the singletonVal here, because if we had to go back more
                // rows, then whatever the correct value was, was already set at the initial location.
                singletonLastStamp = locateFirstValidPreviousTimestamp(sourceIndex, firstUnmodifiedKey);
            }
        }
    }

    @Override
    public void resetForReprocess(@NotNull final UpdateContext ctx,
            @NotNull final RowSet bucketIndex,
            final long bucketPosition,
            final long firstUnmodifiedKey) {
        final BigDecimal previousVal = firstUnmodifiedKey == NULL_ROW_KEY ? null : outputSource.get(firstUnmodifiedKey);
        bucketLastVal.set(bucketPosition, previousVal);

        if (timeRecorder == null) {
            return;
        }

        long potentialResetTimestamp;
        if (previousVal == null) {
            potentialResetTimestamp = NULL_LONG;
        } else {
            // If it hasn't been reset to null, then it's possible that the value at that position was null, in which
            // case
            // we must have ignored it, and so we have to actually keep looking backwards until we find something
            // not null.


            // Note that it's OK that we are not setting the singletonVal here, because if we had to go back more
            // rows, then whatever the correct value was, was already set at the initial location.
            potentialResetTimestamp = locateFirstValidPreviousTimestamp(bucketIndex, firstUnmodifiedKey);
        }
        bucketLastTimestamp.set(bucketPosition, potentialResetTimestamp);
    }

    private long locateFirstValidPreviousTimestamp(@NotNull final RowSet indexToSearch,
            final long firstUnmodifiedKey) {
        long potentialResetTimestamp = timeRecorder.getCurrentLong(firstUnmodifiedKey);
        if (potentialResetTimestamp != NULL_LONG && isValueValid(firstUnmodifiedKey)) {
            return potentialResetTimestamp;
        }

        try (final RowSet.SearchIterator rIt = indexToSearch.reverseIterator()) {
            rIt.advance(firstUnmodifiedKey);
            while (rIt.hasNext()) {
                final long nextKey = rIt.nextLong();
                potentialResetTimestamp = timeRecorder.getCurrentLong(nextKey);
                if (potentialResetTimestamp != NULL_LONG && isValueValid(nextKey)) {
                    return potentialResetTimestamp;
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

/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseWindowedFloatUpdateByOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.LongRingBuffer;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseWindowedDoubleUpdateByOperator extends UpdateByWindowedOperator {
    protected final ColumnSource<Double> valueSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateWindowedContext {
        public boolean canProcessDirectly;

        public WritableDoubleChunk<Values> candidateValuesChunk;

        // other useful stuff
        public UpdateBy.UpdateType currentUpdateType;

        public RowSetBuilderSequential modifiedBuilder;
        public RowSet newModified;

        public RowSetBuilderSequential getModifiedBuilder() {
            if(modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        @Override
        public void close() {
            super.close();
            if (candidateValuesChunk != null) {
                candidateValuesChunk.close();
                candidateValuesChunk = null;
            }
        }

        @Override
        public void loadCandidateValueChunk(RowSequence windowRowSequence) {
            // fill the window values chunk
            if (candidateValuesChunk == null) {
                candidateValuesChunk = WritableDoubleChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
            }
            try (ChunkSource.FillContext fc = valueSource.makeFillContext(WINDOW_CHUNK_SIZE)){
                valueSource.fillChunk(fc, candidateValuesChunk, windowRowSequence);
            }
        }
    }

    public BaseWindowedDoubleUpdateByOperator(@NotNull final MatchPair pair,
                                             @NotNull final String[] affectingColumns,
                                             @NotNull final OperationControl control,
                                             @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                             final long reverseTimeScaleUnits,
                                             final long forwardTimeScaleUnits,
                                             @Nullable final RowRedirection rowRedirection,
                                             @NotNull final ColumnSource<Double> valueSource
                                             // region extra-constructor-args
                                             // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, control, timeRecorder, reverseTimeScaleUnits, forwardTimeScaleUnits, rowRedirection);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    @Override
    public void initializeForUpdate(@NotNull UpdateContext context, @NotNull TableUpdate upstream, @NotNull RowSet resultSourceRowSet, final long lastPrevKey, boolean isUpstreamAppendOnly) {
        final Context ctx = (Context) context;
        ctx.workingRowSet = resultSourceRowSet;

        // we can only process directly from an update if the window is entire backward-looking.  Since we
        // allow negative values in fwd/rev ticks and timestamps, we need to check both
        ctx.canProcessDirectly = isUpstreamAppendOnly && forwardTimeScaleUnits <= 0 && reverseTimeScaleUnits >= 0;;
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
                              @NotNull final RowSet updateRowSet,
                              @NotNull final UpdateBy.UpdateType type) {
        final Context ctx = (Context) context;
        ctx.currentUpdateType = type;

        if ((type == UpdateBy.UpdateType.Add && ctx.canProcessDirectly)
                || type == UpdateBy.UpdateType.Reprocess) {
            long windowStartKey = computeFirstAffectingKey(updateRowSet.firstRowKey(), ctx.workingRowSet);
            ctx.loadWindowChunks(windowStartKey);
        }
    }

    @Override
    public void finishFor(@NotNull final UpdateContext updateContext, @NotNull final UpdateBy.UpdateType type) {
        final Context ctx = (Context) updateContext;
        if(type == UpdateBy.UpdateType.Reprocess && ctx.modifiedBuilder != null) {
            ctx.newModified = ctx.modifiedBuilder.build();
        }
    }

    @Override
    public boolean canProcessNormalUpdate(@NotNull UpdateContext context) {
        final Context ctx = (Context) context;
        return ctx.canProcessDirectly;
    }

    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        // windowed operators don't need current values supplied to them, they only care about windowed values which
        // may or may not intersect with the column values
        return false;
    }

    @NotNull
    @Override
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext ctx) {
        return ((Context)ctx).newModified == null ? RowSetFactory.empty() : ((Context)ctx).newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext ctx) {
        return ((Context)ctx).newModified != null;
    }

    // region Addition
    /**
     * Add a chunk of values to the operator.
     *
     * @param ctx the context object
     * @param inputKeys the input keys for the chunk
     * @param workingChunk the chunk of values
     */
    protected abstract void doAddChunk(@NotNull final Context ctx,
                                       @NotNull final RowSequence inputKeys,
                                       @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                       @NotNull final Chunk<Values> workingChunk);

    @Override
    public void addChunk(@NotNull final UpdateContext updateContext,
                                 @NotNull final RowSequence inputKeys,
                                 @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                 @NotNull final Chunk<Values> values) {
        final Context ctx = (Context) updateContext;
        if (ctx.canProcessDirectly) {
            ctx.loadDataChunks(inputKeys);
            doAddChunk(ctx, inputKeys, keyChunk, values);
        }
    }

    // endregion

    // region Reprocessing

    public void resetForReprocess(@NotNull final UpdateContext context,
                                  @NotNull final RowSet sourceRowSet,
                                  long firstUnmodifiedKey) {
        final Context ctx = (Context) context;
        ctx.workingRowSet = sourceRowSet;
    }

    @Override
    public void reprocessChunk(@NotNull final UpdateContext updateContext,
                                       @NotNull final RowSequence inputKeys,
                                       @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                       @NotNull final Chunk<Values> valuesChunk,
                                       @NotNull final RowSet postUpdateSourceIndex) {
        final Context ctx = (Context) updateContext;
        ctx.loadDataChunks(inputKeys);
        doAddChunk(ctx, inputKeys, keyChunk, valuesChunk);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }

    // endregion

    // region No-Op Operations

    @Override
    final public void modifyChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> prevKeyChunk,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk,
                                  @NotNull final Chunk<Values> postValuesChunk) {
    }

    @Override
    final public void removeChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk) {
    }

    @Override
    final public void applyShift(@NotNull final UpdateContext updateContext,
                                 @NotNull final RowSet prevIndex,
                                 @NotNull final RowSetShiftData shifted) {
    }
    // endregion
}

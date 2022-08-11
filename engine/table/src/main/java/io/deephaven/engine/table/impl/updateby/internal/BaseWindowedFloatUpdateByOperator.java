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

public abstract class BaseWindowedFloatUpdateByOperator extends UpdateByWindowedOperator {
    protected final ColumnSource<Float> valueSource;

    protected boolean initialized = false;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateWindowedContext {
        public boolean canProcessDirectly;
        public RowSet workingRowSet = null;

        // candidate data for the window
        public final int WINDOW_CHUNK_SIZE = 1024;

        // data that is actually in the current window
        public LongRingBuffer windowRowKeys = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        // the selector that determines whether this value should be in the window, positions for tick-based and
        // timestamps for time-based operators
        public LongRingBuffer windowSelector = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        public RowSequence.Iterator windowIterator = null;

        public WritableFloatChunk<Values> candidateValuesChunk;
        public WritableLongChunk<? extends RowKeys> candidateRowKeysChunk;
        public WritableLongChunk<? extends RowKeys> candidatePositionsChunk;
        public WritableLongChunk<Values> candidateTimestampsChunk;

        public int candidateWindowIndex = 0;

        // position data for the chunk being currently processed
        public WritableLongChunk<? extends RowKeys> valuePositionChunk;

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
            if (windowIterator != null) {
                windowIterator.close();
                windowIterator = null;
            }

            if (candidateValuesChunk != null) {
                candidateValuesChunk.close();
                candidateValuesChunk = null;
            }

            if (candidateRowKeysChunk != null) {
                candidateRowKeysChunk.close();
                candidateRowKeysChunk = null;
            }

            if (candidatePositionsChunk != null) {
                candidatePositionsChunk.close();
                candidatePositionsChunk = null;
            }

            if (valuePositionChunk != null) {
                valuePositionChunk.close();
                valuePositionChunk = null;
            }

            // no need to close, just release the reference
            workingRowSet = null;
        }

        /***
         * Fill the working chunks with data for this key
         *
         * @param startKey the key for which we want to
         */
        public void loadWindowChunks(final long startKey) {
            // TODO: make sure this works for bucketed
            if (windowIterator == null) {
                windowIterator = workingRowSet.getRowSequenceIterator();
            }
            windowIterator.advance(startKey);

            RowSequence windowRowSequence = windowIterator.getNextRowSequenceWithLength(WINDOW_CHUNK_SIZE);

            // fill the window values chunk
            if (candidateValuesChunk == null) {
                candidateValuesChunk = WritableFloatChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
            }
            try (ChunkSource.FillContext fc = valueSource.makeFillContext(WINDOW_CHUNK_SIZE)){
                valueSource.fillChunk(fc, candidateValuesChunk, windowRowSequence);
            }

            // fill the window keys chunk
            if (candidateRowKeysChunk == null) {
                candidateRowKeysChunk = WritableLongChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
            }
            windowRowSequence.fillRowKeyChunk(candidateRowKeysChunk);

            if (recorder == null) {
                // get position data for the window items (relative to the table or bucket rowset)
                if (candidatePositionsChunk == null) {
                    candidatePositionsChunk = WritableLongChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
                }

                // TODO: gotta be a better way than creating two rowsets
                try (final RowSet rs = windowRowSequence.asRowSet();
                     final RowSet positions = workingRowSet.invert(rs)) {
                    positions.fillRowKeyChunk(candidatePositionsChunk);
                }
            } else {
                // get timestamp values from the recorder column source
                if (candidateTimestampsChunk == null) {
                    candidateTimestampsChunk = WritableLongChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
                }
                try (final ChunkSource.FillContext fc = recorder.getColumnSource().makeFillContext(WINDOW_CHUNK_SIZE)) {
                    recorder.getColumnSource().fillChunk(fc, candidateTimestampsChunk, windowRowSequence);
                }
            }

            // reset the index to beginning of the chunks
            candidateWindowIndex = 0;
        }

        /***
         * Fill the working chunks with data for this key
         *
         * @param inputKeys the keys for which we want to get position or timestamp values
         */
        public void loadDataChunks(final RowSequence inputKeys) {
            if (recorder != null) {
                // timestamp data will be available from the recorder
                return;
            }

            if (valuePositionChunk == null) {
                valuePositionChunk = WritableLongChunk.makeWritableChunk(inputKeys.intSize());
            } else if (valuePositionChunk.capacity() < inputKeys.size()) {
                valuePositionChunk.close();
                valuePositionChunk = WritableLongChunk.makeWritableChunk(inputKeys.intSize());
            }

            // produce position data for the window (will be timestamps for time-based)
            // TODO: gotta be a better way than creating two rowsets
            try (final RowSet rs = inputKeys.asRowSet();
                 final RowSet positions = workingRowSet.invert(rs)) {
                positions.fillRowKeyChunk(valuePositionChunk);
            }
        }

        public void fillWindowTicks(UpdateContext context, long currentPos) {
            // compute the head and tail (inclusive)
            final long tail = Math.max(0, currentPos - reverseTimeScaleUnits + 1);
            final long head = Math.min(workingRowSet.size() - 1, currentPos + forwardTimeScaleUnits);

            while (windowSelector.peek(Long.MAX_VALUE) < tail) {
                final long pos = windowSelector.remove();
                final long key = windowRowKeys.remove();

                pop(context, key);
            }


            // look at the window data and push until satisfied or at the end of the rowset
            while (candidatePositionsChunk.size() > 0 && candidatePositionsChunk.get(candidateWindowIndex) <= head) {
                final long pos = candidatePositionsChunk.get(candidateWindowIndex);
                final long key = candidateRowKeysChunk.get(candidateWindowIndex);
                final float val = candidateValuesChunk.get(candidateWindowIndex);

                push(context, key, val);

                windowSelector.add(pos);
                windowRowKeys.add(key);

                if (++candidateWindowIndex >= candidatePositionsChunk.size()) {
                    // load the next chunk in order
                    loadWindowChunks(key + 1);
                }
            }

            if (windowSelector.isEmpty()) {
                reset(context);
            }
        }
    }

    public BaseWindowedFloatUpdateByOperator(@NotNull final MatchPair pair,
                                             @NotNull final String[] affectingColumns,
                                             @NotNull final OperationControl control,
                                             @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                             final long reverseTimeScaleUnits,
                                             final long forwardTimeScaleUnits,
                                             @Nullable final RowRedirection rowRedirection,
                                             @NotNull final ColumnSource<Float> valueSource
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

    public abstract void push(UpdateContext context, long key, float val);
    public abstract void pop(UpdateContext context, long key);
    public abstract void reset(UpdateContext context);

    @Override
    public void initializeForUpdate(@NotNull UpdateContext context, @NotNull TableUpdate upstream, @NotNull RowSet resultSourceRowSet, boolean usingBuckets, boolean isUpstreamAppendOnly) {
        final Context ctx = (Context) context;
        ctx.workingRowSet = resultSourceRowSet;

        if(!usingBuckets) {
            // we can only process directly from an update if the window is entire backward-looking.  Since we
            // allow negative values in fwd/rev ticks and timestamps, we need to check both
            ctx.canProcessDirectly = isUpstreamAppendOnly && forwardTimeScaleUnits <= 0 && reverseTimeScaleUnits >= 0;;
        }
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
     * @param bucketPosition the bucket position that the values belong to.
     */
    protected abstract void doAddChunk(@NotNull final Context ctx,
                                       @NotNull final RowSequence inputKeys,
                                       @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                       @NotNull final Chunk<Values> workingChunk,
                                       final long bucketPosition);

    @Override
    public void addChunk(@NotNull final UpdateContext updateContext,
                                 @NotNull final RowSequence inputKeys,
                                 @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                 @NotNull final Chunk<Values> values,
                                 long bucketPosition) {
        final Context ctx = (Context) updateContext;
        if (ctx.canProcessDirectly) {
            ctx.loadDataChunks(inputKeys);
            doAddChunk(ctx, inputKeys, keyChunk, values, bucketPosition);
        }
    }

    @Override
    public void addChunkBucketed(final @NotNull UpdateContext context,
                                 final @NotNull Chunk<Values> values,
                                 final @NotNull LongChunk<? extends RowKeys> keyChunk,
                                 final @NotNull IntChunk<RowKeys> bucketPositions,
                                 final @NotNull IntChunk<ChunkPositions> startPositions,
                                 final @NotNull IntChunk<ChunkLengths> runLengths) {
        final Context ctx = (Context) context;
        if (ctx.canProcessDirectly) {
//            final ShortChunk<Values> asShorts = values.asShortChunk();
            for (int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
                final int runStart = startPositions.get(runIdx);
                final int runLength = runLengths.get(runIdx);
                final int bucketPosition = bucketPositions.get(runStart);
//                doAddChunk(ctx, inputKeys, keyChunk, values, bucketPosition);

//            try (RowSequence rs = RowSequenceFactory.wrapRowKeysChunkAsRowSequence((LongChunk<OrderedRowKeys>) keyChunk)_

//            RowSetBuilderSequential builder = RowSetFactory.builderSequential();
//            for (int ii = runStart; ii < runStart + runLength; ii++) {
//                builder.appendKey(keyChunk.get(ii));
//            }
//
//            WritableRowSet bucketRs = bucketRowSet.get(bucketPosition);
//            if (bucketRs == null) {
//                bucketRs = builder.build();
//                bucketRowSet.set(bucketPosition, bucketRs);
//            } else {
//                try (final RowSet added = builder.build()) {
//                    bucketRs.insert(added);
//                }
//            }
//
//            ctx.curVal = NULL_LONG;
//            ctx.currentWindow.clear();

//            accumulate(asShorts, (LongChunk<OrderedRowKeys>) keyChunk, ctx, runStart, runLength, bucketRs);
//            bucketLastVal.set(bucketPosition, ctx.curVal);
            }
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
    public void resetForReprocessBucketed(@NotNull final UpdateContext context,
                                          @NotNull final RowSet bucketRowSet,
                                          final long bucketPosition,
                                          final long firstUnmodifiedKey) {
        final Context ctx = (Context) context;
    }

    @Override
    public void reprocessChunk(@NotNull final UpdateContext updateContext,
                                       @NotNull final RowSequence inputKeys,
                                       @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                       @NotNull final Chunk<Values> valuesChunk,
                                       @NotNull final RowSet postUpdateSourceIndex) {
        final Context ctx = (Context) updateContext;
        ctx.loadDataChunks(inputKeys);
        doAddChunk(ctx, inputKeys, keyChunk, valuesChunk, 0);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }

    @Override
    public void reprocessChunkBucketed(@NotNull UpdateContext updateContext,
                                       @NotNull final RowSequence chunkOk,
                                       @NotNull final Chunk<Values> values,
                                       @NotNull final LongChunk<? extends RowKeys> keyChunk,
                                       @NotNull final IntChunk<RowKeys> bucketPositions,
                                       @NotNull final IntChunk<ChunkPositions> runStartPositions,
                                       @NotNull final IntChunk<ChunkLengths> runLengths) {
        addChunkBucketed(updateContext, values, keyChunk, bucketPositions, runStartPositions, runLengths);
        ((Context)updateContext).getModifiedBuilder().appendRowSequence(chunkOk);
    }

    // endregion

    // region No-Op Operations

    @Override
    final public void modifyChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> prevKeyChunk,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk,
                                  @NotNull final Chunk<Values> postValuesChunk,
                                  long bucketPosition) {
    }

    @Override
    final public void removeChunk(@NotNull final UpdateContext updateContext,
                                  @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                  @NotNull final Chunk<Values> prevValuesChunk,
                                  long bucketPosition) {
    }

    @Override
    final public void applyShift(@NotNull final UpdateContext updateContext,
                                 @NotNull final RowSet prevIndex,
                                 @NotNull final RowSetShiftData shifted) {
    }
    // endregion
}

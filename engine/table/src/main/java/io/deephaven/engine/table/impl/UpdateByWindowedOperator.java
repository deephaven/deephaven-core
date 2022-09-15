package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class UpdateByWindowedOperator implements UpdateByOperator {
    protected final OperationControl control;
    protected final String timestampColumnName;
    protected final ColumnSource<?> timestampColumnSource;

    protected final long reverseTimeScaleUnits;
    protected final long forwardTimeScaleUnits;

    protected final MatchPair pair;
    protected final String[] affectingColumns;

    protected UpdateBy.UpdateByRedirectionContext redirContext;

    public abstract class UpdateWindowedContext implements UpdateContext {
        protected LongSegmentedSortedArray timestampSsa;

        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        public int nullCount = 0;

        // store a local copy of the source rowset (may not be needed)
        public RowSet sourceRowSet = null;

        // there are two sets of rows we will be tracking. `affected` rows need to be recomputed because of this
        // update and `influencer` rows contain the data that will be used to compute the new values for the `affected`
        // items. Because the windows are user-configurable, there may be no overlap between these two sets and we
        // don't need values for the `affected` rows at all
        protected RowSet affectedRows;
        protected RowSet influencerRows;
        protected long currentInfluencerKey;

        // candidate data for the window
        public final int WINDOW_CHUNK_SIZE = 4096;

        // allocate some chunks for holding the key, position and timestamp data
        protected SizedLongChunk<RowKeys> influencerKeyChunk;
        protected SizedLongChunk<RowKeys> influencerPosChunk;
        protected SizedLongChunk<? extends Values> influencerTimestampChunk;

        // for use with a ticking window
        protected RowSet affectedRowPositions;
        protected RowSet influencerPositions;

        protected long currentInfluencerPosOrTimestamp;
        protected int currentInfluencerIndex;

        protected LongRingBuffer windowKeys = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        protected LongRingBuffer windowPosOrTimestamp = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        public LongRingBuffer windowIndices = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        private WritableRowSet computeAffectedRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
                long fwdNanos) {
            // swap fwd/rev to get the affected windows
            return computeInfluencerRowsTime(sourceSet, subset, fwdNanos, revNanos);
        }

        private WritableRowSet computeInfluencerRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
                long fwdNanos) {
            if (sourceSet.size() == subset.size()) {
                return sourceSet.copy();
            }

            int chunkSize = (int) Math.min(subset.size(), 4096);
            try (final RowSequence.Iterator it = subset.getRowSequenceIterator();
                    final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(chunkSize)) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                LongSegmentedSortedArray.Iterator ssaIt = timestampSsa.iterator(false, false);
                while (it.hasMore() && ssaIt.hasNext()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    LongChunk<? extends Values> timestamps = timestampColumnSource.getChunk(context, rs).asLongChunk();

                    for (int ii = 0; ii < rs.intSize(); ii++) {
                        // if the timestamp of the row is null, it won't belong to any set and we can ignore it
                        // completely
                        final long ts = timestamps.get(ii);
                        if (ts != NULL_LONG) {
                            // look at every row timestamp, compute the head and tail in nanos
                            final long head = ts - revNanos;
                            final long tail = ts + fwdNanos;

                            // advance the iterator to the beginning of the window
                            if (ssaIt.nextValue() < head) {
                                ssaIt.advanceToBeforeFirst(head);
                                if (!ssaIt.hasNext()) {
                                    // SSA is exhausted
                                    break;
                                }
                            }

                            Assert.assertion(ssaIt.hasNext() && ssaIt.nextValue() >= head,
                                    "SSA Iterator outside of window");

                            // step through the SSA and collect keys until outside of the window
                            while (ssaIt.hasNext() && ssaIt.nextValue() <= tail) {
                                builder.appendKey(ssaIt.nextKey());
                                ssaIt.next();
                            }

                            if (!ssaIt.hasNext()) {
                                // SSA is exhausted
                                break;
                            }
                        }
                    }
                }
                return builder.build();
            }
        }

        private WritableRowSet computeAffectedRowsTicks(final RowSet sourceSet, final RowSet subset, long revTicks,
                long fwdTicks) {
            // swap fwd/rev to get the influencer windows
            return computeInfluencerRowsTicks(sourceSet, subset, fwdTicks, revTicks);
        }

        private WritableRowSet computeInfluencerRowsTicks(final RowSet sourceSet, final RowSet subset, long revTicks,
                long fwdTicks) {
            if (sourceSet.size() == subset.size()) {
                return sourceSet.copy();
            }

            long maxPos = sourceSet.size() - 1;

            try (final RowSet inverted = sourceSet.invert(subset)) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                final MutableLong minPos = new MutableLong(0L);

                inverted.forAllRowKeyRanges((s, e) -> {
                    long sPos = Math.max(s - revTicks, minPos.longValue());
                    long ePos = Math.min(e + fwdTicks, maxPos);
                    builder.appendRange(sPos, ePos);
                    minPos.setValue(ePos + 1);
                });

                try (final RowSet positions = builder.build()) {
                    return sourceSet.subSetForPositions(positions);
                }
            }
        }

        /***
         * This function is only correct if the proper {@code source} rowset is provided. If using buckets, then the
         * provided rowset must be limited to the rows in the current bucket only
         *
         * @param upstream the update
         * @param source the rowset of the parent table (affected rows will be a subset)
         * @param initialStep whether this is the initial step of building the table
         */
        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source,
                final boolean initialStep) {
            Assert.assertion(affectedRows == null,
                    "affectedRows should be null when determineAffectedRows() is called");

            if (initialStep) {
                // all rows are affected initially
                affectedRows = source.copy();
                influencerRows = source.copy();

                // no need to invert, just create a flat rowset
                if (timestampColumnName == null) {
                    affectedRowPositions = RowSetFactory.flat(source.size());
                    influencerPositions = RowSetFactory.flat(source.size());
                }
                return affectedRows;
            }

            if (source.isEmpty()) {
                affectedRows = RowSetFactory.empty();
                influencerRows = RowSetFactory.empty();
                if (timestampColumnName == null) {
                    affectedRowPositions = RowSetFactory.empty();
                    influencerPositions = RowSetFactory.empty();
                }
                return affectedRows;
            }

            // changed rows are mods+adds
            WritableRowSet changed = upstream.added().copy();
            changed.insert(upstream.modified());

            WritableRowSet tmpAffected;

            // compute the affected rows from these changes
            if (timestampColumnName == null) {
                tmpAffected = computeAffectedRowsTicks(source, changed, reverseTimeScaleUnits, forwardTimeScaleUnits);
            } else {
                tmpAffected = computeAffectedRowsTime(source, changed, reverseTimeScaleUnits, forwardTimeScaleUnits);
            }

            // add affected rows from any removes

            if (upstream.removed().isNonempty()) {
                try (final RowSet prev = source.copyPrev();
                        final WritableRowSet affectedByRemoves = timestampColumnName == null
                                ? computeAffectedRowsTicks(prev, upstream.removed(), reverseTimeScaleUnits,
                                        forwardTimeScaleUnits)
                                : computeAffectedRowsTime(prev, upstream.removed(), reverseTimeScaleUnits,
                                        forwardTimeScaleUnits)) {
                    // apply shifts to get back to pos-shift space
                    upstream.shifted().apply(affectedByRemoves);
                    // retain only the rows that still exist in the source
                    affectedByRemoves.retain(source);
                    tmpAffected.insert(affectedByRemoves);
                }
            }

            affectedRows = tmpAffected;

            // now get influencer rows for the affected

            if (timestampColumnName == null) {
                influencerRows =
                        computeInfluencerRowsTicks(source, affectedRows, reverseTimeScaleUnits, forwardTimeScaleUnits);
                // generate position data rowsets for efficiently computed position offsets
                affectedRowPositions = source.invert(affectedRows);
                influencerPositions = source.invert(influencerRows);
            } else {
                influencerRows =
                        computeInfluencerRowsTime(source, affectedRows, reverseTimeScaleUnits, forwardTimeScaleUnits);
            }
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSet getInfluencerRows() {
            return influencerRows;
        }

        public LongSegmentedSortedArray getTimestampSsa() {
            return timestampSsa;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if (modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        public abstract void loadInfluencerValueChunk();

        public void fillWindowTicks(UpdateWindowedContext context, long currentPos) {
            // compute the head and tail positions (inclusive)
            final long head = Math.max(0, currentPos - reverseTimeScaleUnits + 1);
            final long tail = Math.min(sourceRowSet.size() - 1, currentPos + forwardTimeScaleUnits);

            // pop out all values from the current window that are not in the new window
            while (!windowPosOrTimestamp.isEmpty() && windowPosOrTimestamp.front() < head) {
                pop(context, windowKeys.remove(), (int) windowIndices.remove());
                windowPosOrTimestamp.remove();
            }

            // if the window is empty or completly filled with null, call reset()
            if (windowPosOrTimestamp.isEmpty() || context.nullCount == windowPosOrTimestamp.size()) {
                reset(context);
            }

            // skip values until they match the window
            while (currentInfluencerPosOrTimestamp < head) {
                currentInfluencerIndex++;

                if (currentInfluencerIndex < influencerPosChunk.get().size()) {
                    currentInfluencerPosOrTimestamp = influencerPosChunk.get().get(currentInfluencerIndex);
                    currentInfluencerKey = influencerKeyChunk.get().get(currentInfluencerIndex);
                } else {
                    currentInfluencerPosOrTimestamp = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }

            // push matching values
            while (currentInfluencerPosOrTimestamp <= tail) {
                push(context, currentInfluencerKey, currentInfluencerIndex);
                windowKeys.add(currentInfluencerKey);
                windowPosOrTimestamp.add(currentInfluencerPosOrTimestamp);
                windowIndices.add(currentInfluencerIndex);
                currentInfluencerIndex++;

                if (currentInfluencerIndex < influencerPosChunk.get().size()) {
                    currentInfluencerPosOrTimestamp = influencerPosChunk.get().get(currentInfluencerIndex);
                    currentInfluencerKey = influencerKeyChunk.get().get(currentInfluencerIndex);
                } else {
                    currentInfluencerPosOrTimestamp = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }
        }

        public void fillWindowTime(UpdateWindowedContext context, long currentTimestamp) {
            // compute the head and tail positions (inclusive)
            final long head = currentTimestamp - reverseTimeScaleUnits;
            final long tail = currentTimestamp + forwardTimeScaleUnits;

            // pop out all values from the current window that are not in the new window
            while (!windowPosOrTimestamp.isEmpty() && windowPosOrTimestamp.front() < head) {
                pop(context, windowKeys.remove(), (int) windowIndices.remove());
                windowPosOrTimestamp.remove();
            }

            // if the window is empty or completly filled with null, call reset()
            if (windowPosOrTimestamp.isEmpty() || context.nullCount == windowPosOrTimestamp.size()) {
                reset(context);
            }

            // skip values until they match the window
            while (currentInfluencerPosOrTimestamp < head) {
                currentInfluencerIndex++;

                if (currentInfluencerIndex < influencerTimestampChunk.get().size()) {
                    currentInfluencerPosOrTimestamp = influencerTimestampChunk.get().get(currentInfluencerIndex);
                    currentInfluencerKey = influencerKeyChunk.get().get(currentInfluencerIndex);
                } else {
                    currentInfluencerPosOrTimestamp = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }

            // push matching values
            while (currentInfluencerPosOrTimestamp <= tail) {
                push(context, currentInfluencerKey, currentInfluencerIndex);
                windowKeys.add(currentInfluencerKey);
                windowPosOrTimestamp.add(currentInfluencerPosOrTimestamp);
                windowIndices.add(currentInfluencerIndex);
                currentInfluencerIndex++;

                if (currentInfluencerIndex < influencerTimestampChunk.get().size()) {
                    currentInfluencerPosOrTimestamp = influencerTimestampChunk.get().get(currentInfluencerIndex);
                    currentInfluencerKey = influencerKeyChunk.get().get(currentInfluencerIndex);
                } else {
                    currentInfluencerPosOrTimestamp = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }
        }

        @Override
        public void close() {
            try (final SizedLongChunk<RowKeys> ignoredChk1 = influencerKeyChunk;
                    final SizedLongChunk<RowKeys> ignoredChk2 = influencerPosChunk;
                    final SizedLongChunk<? extends Values> ignoredChk3 = influencerTimestampChunk;
                    final RowSet ignoredRs1 = affectedRows;
                    final RowSet ignoredRs2 = influencerRows;
                    final RowSet ignoredRs3 = affectedRowPositions;
                    final RowSet ignoredRs4 = influencerPositions;
                    final RowSet ignoredRs5 = newModified) {
            }
        }
    }

    /**
     * An operator that computes a windowed operation from a column
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this operation
     * @param control the control parameters for operation
     * @param timestampColumnName the optional time stamp column for windowing (uses ticks if not provided)
     * @param redirContext the row redirection context to use for the operation
     */
    public UpdateByWindowedOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            @Nullable final ColumnSource<?> timestampColumnSource,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.control = control;
        this.timestampColumnName = timestampColumnName;
        this.timestampColumnSource =
                timestampColumnSource == null ? null : ReinterpretUtils.maybeConvertToPrimitive(timestampColumnSource);
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.redirContext = redirContext;
    }

    public abstract void push(UpdateContext context, long key, int pos);

    public abstract void pop(UpdateContext context, long key, int pos);

    public abstract void reset(UpdateContext context);

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
            @NotNull final RowSet updateRowSet) {
        final UpdateWindowedContext ctx = (UpdateWindowedContext) context;

        // load all the influencer values this update will need
        ctx.loadInfluencerValueChunk();

        // load all the influencer keys
        ctx.influencerKeyChunk = new SizedLongChunk<>(ctx.influencerRows.intSize());
        ctx.influencerRows.fillRowKeyChunk(ctx.influencerKeyChunk.get());
        ctx.currentInfluencerKey = ctx.influencerRows.firstRowKey();

        if (timestampColumnName == null) {
            // load all the influencer positions
            ctx.influencerPosChunk = new SizedLongChunk<>(ctx.influencerRows.intSize());
            ctx.influencerPositions.fillRowKeyChunk(ctx.influencerPosChunk.get());
            ctx.currentInfluencerPosOrTimestamp = ctx.influencerPositions.firstRowKey();
        } else {
            // load all the influencer timestamp data
            ctx.influencerTimestampChunk = new SizedLongChunk<>(ctx.influencerRows.intSize());
            try (final ChunkSource.FillContext fillContext =
                    timestampColumnSource.makeFillContext(ctx.influencerRows.intSize())) {
                timestampColumnSource.fillChunk(fillContext,
                        (WritableChunk<? super Values>) ctx.influencerTimestampChunk.get(), ctx.influencerRows);
            }
            ctx.currentInfluencerPosOrTimestamp = ctx.influencerTimestampChunk.get().get(0);
        }
        ctx.currentInfluencerIndex = 0;
    }

    @Override
    public void finishFor(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext) context;
        ctx.newModified = ctx.getModifiedBuilder().build();
    }

    @NotNull
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext) context;
        return ctx.newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext) context;
        return ctx.newModified != null && ctx.newModified.isNonempty();
    }

    @NotNull
    @Override
    public String getInputColumnName() {
        return pair.rightColumn;
    }

    @NotNull
    @Override
    public String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    @NotNull
    @Override
    public String[] getOutputColumnNames() {
        return new String[] {pair.leftColumn};
    }

    @Override
    public String getTimestampColumnName() {
        return this.timestampColumnName;
    }

    @Override
    public boolean requiresKeys() {
        return false;
    }

    /*** windowed operators need position data when computing ticks */
    @Override
    public boolean requiresPositions() {
        return this.timestampColumnName == null;
    }

    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        // windowed operators don't need current values supplied to them, they only care about windowed values which
        // may or may not intersect with the column values
        return false;
    }
}

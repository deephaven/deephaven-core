package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
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

        public LongSegmentedSortedArray getTimestampSsa() {
            return timestampSsa;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if (modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        // store a local copy of the source rowset (may not be needed)
        public RowSet sourceRowSet = null;

        // there are two sets of rows we will be tracking.  `affected` rows need to be recomputed because of this
        // update and `influencer` rows contain the data that will be used to compute the new values for the `affected`
        // items.  Because the windows are user-configurable, there may be no overlap between these two sets and we
        // don't need values for the `affected` rows at all
        protected RowSet affectedRows;
        protected RowSet influencerRows;
        protected long currentInfluencerKey;

        // candidate data for the window
        public final int WINDOW_CHUNK_SIZE = 4096;

        // persist two iterators, for keys and positions
        protected SizedLongChunk<RowKeys> influencerKeyChunk;
        protected SizedLongChunk<RowKeys> influencerPosChunk;
        protected LongSegmentedSortedArray.Iterator ssaIterator;

        // for use with a ticking window
        protected RowSet affectedRowPositions;
        protected RowSet influencerPositions;
        protected long currentInfluencerPosOrTimestamp;
        protected int currentInfluencerIndex;

        protected LongRingBuffer windowKeys = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        protected LongRingBuffer windowPosOrTimestamp = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        protected LongRingBuffer windowIndices = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        private WritableRowSet computeAffectedRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos, long fwdNanos) {
            // swap fwd/rev to get the influencer windows
            return computeInfluencerRowsTicks(sourceSet, subset, fwdNanos, revNanos);
        }

        private WritableRowSet computeInfluencerRowsTime(final RowSet sourceSet, final RowSet subset, long revTicks, long fwdTicks) {
            if (sourceSet.size() == subset.size()) {
                return sourceSet.copy();
            }

            int chunkSize = (int) Math.min(subset.size(), 4096);
            try (ChunkSource.GetContext getContext = timestampColumnSource.makeGetContext(chunkSize);
                 RowSequence.Iterator chunkIt = subset.getRowSequenceIterator()) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                final LongSegmentedSortedArray.Iterator ssaHeadIt = timestampSsa.iterator(false, false);
                final LongSegmentedSortedArray.Iterator ssaTailIt = timestampSsa.iterator(false, false);
                while (chunkIt.hasMore()) {
                    RowSequence chunkRs = chunkIt.getNextRowSequenceWithLength(chunkSize);
                    LongChunk<? extends Values> tsChunk = timestampColumnSource.getPrevChunk(getContext, chunkRs).asLongChunk();
                    for (int i = 0; i < tsChunk.size(); i++) {
                        long ts = tsChunk.get(i);
                        // if this timestamp was null, it wasn't included in any windows and there is nothing to recompute
                        if (ts != NULL_LONG) {
                            ssaHeadIt.advanceToBeforeFirst(ts);
                            final long s = ssaHeadIt.getKey();
                            ssaTailIt.advanceToLast(ts);
                            final long e;
                            if (ssaHeadIt.hasNext()) {
                                e = ssaTailIt.nextKey();
                            } else {
                                e = NULL_LONG;
                            }
                            builder.appendRange(s, e);
                        }
                    }
                }
                try (final RowSet removedChanges = builder.build()) {
                   // changed.insert(removedChanges);
                }
            }



            long maxPos = sourceSet.size() - 1;

            // find the first row

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


        private WritableRowSet computeAffectedRowsTicks(final RowSet sourceSet, final RowSet subset, long revTicks, long fwdTicks) {
            // swap fwd/rev to get the influencer windows
            return computeInfluencerRowsTicks(sourceSet, subset, fwdTicks, revTicks);
        }

        private WritableRowSet computeInfluencerRowsTicks(final RowSet sourceSet, final RowSet subset, long revTicks, long fwdTicks) {
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
         * This function is only correct if the proper {@code source} rowset is provided.  If using buckets, then the
         * provided rowset must be limited to the rows in the current bucket
         * only
         *
         * @param upstream the update
         * @param source the rowset of the parent table (affected rows will be a subset)
         * @param initialStep whether this is the initial step of building the table
         */
        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source,
                                            final boolean initialStep) {
            Assert.assertion(affectedRows==null, "affectedRows should be null when determineAffectedRows() is called");

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
                if (timestampColumnName == null) {
                    // tick based
                    try (final RowSet prev = source.copyPrev();
                        final WritableRowSet affectedByRemoves = computeAffectedRowsTicks(prev, upstream.removed(), reverseTimeScaleUnits, forwardTimeScaleUnits )) {
                        // apply shifts to get back to pos-shift space
                        upstream.shifted().apply(affectedByRemoves);
                        // retain only the rows that still exist in the source
                        affectedByRemoves.retain(source);
                        tmpAffected.insert(affectedByRemoves);
                    }
                } else {
                    // time-based, first grab all the timestamp data for these removed rows
                    int size = (int) Math.min(upstream.removed().size(), 4096);
                    try (ChunkSource.GetContext getContext = timestampColumnSource.makeGetContext(size);
                         RowSequence.Iterator chunkIt = upstream.removed().getRowSequenceIterator()) {
                        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                        final LongSegmentedSortedArray.Iterator ssaHeadIt = timestampSsa.iterator(false, false);
                        final LongSegmentedSortedArray.Iterator ssaTailIt = timestampSsa.iterator(false, false);
                        while (chunkIt.hasMore()) {
                            RowSequence chunkRs = chunkIt.getNextRowSequenceWithLength(size);
                            LongChunk<? extends Values> tsChunk = timestampColumnSource.getPrevChunk(getContext, chunkRs).asLongChunk();
                            for (int i = 0; i < tsChunk.size(); i++) {
                                long ts = tsChunk.get(i);
                                // if this timestamp was null, it wasn't included in any windows and there is nothing to recompute
                                if (ts != NULL_LONG) {
                                    ssaHeadIt.advanceToBeforeFirst(ts);
                                    final long s = ssaHeadIt.getKey();
                                    ssaTailIt.advanceToLast(ts);
                                    final long e;
                                    if (ssaHeadIt.hasNext()) {
                                        e = ssaTailIt.nextKey();
                                    } else {
                                        e = NULL_LONG;
                                    }
                                    builder.appendRange(s, e);
                                }
                            }
                        }
                        try (final RowSet removedChanges = builder.build()) {
                            changed.insert(removedChanges);
                        }
                    }
                }
            }

            affectedRows = tmpAffected;

            // now get influencer rows for the affected
            if (timestampColumnName == null) {
                influencerRows = computeInfluencerRowsTicks(source, affectedRows, reverseTimeScaleUnits, forwardTimeScaleUnits);
                // generate position data rowsets for efficiently computed position offsets
                affectedRowPositions = source.invert(affectedRows);
                influencerPositions = source.invert(influencerRows);
            } else {

            }
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public abstract void loadInfluencerValueChunk();

        public void fillWindowTicks(UpdateContext context, long currentPos) {
            // compute the head and tail positions (inclusive)
            final long head = Math.max(0, currentPos - reverseTimeScaleUnits + 1);
            final long tail = Math.min(sourceRowSet.size() - 1, currentPos + forwardTimeScaleUnits);

            // pop out all values from the current window that are not in the new window
            while (!windowPosOrTimestamp.isEmpty() && windowPosOrTimestamp.front() < head) {
                pop(context, windowKeys.remove(), (int)windowIndices.remove());
                windowPosOrTimestamp.remove();
            }

            if (windowPosOrTimestamp.isEmpty()) {
                reset(context);
            }

            // skip values until they match the window
            while(currentInfluencerPosOrTimestamp < head) {
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
            while(currentInfluencerPosOrTimestamp <= tail) {
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

        public void fillWindowTime(UpdateContext context, long currentTimestamp) {
            // compute the head and tail positions (inclusive)
            final long head = currentTimestamp - reverseTimeScaleUnits;
            final long tail = currentTimestamp + forwardTimeScaleUnits;

            // pop out all values from the current window that are not in the new window
            while (!windowPosOrTimestamp.isEmpty() && windowPosOrTimestamp.front() < head) {
                pop(context, windowKeys.remove(), (int)windowIndices.remove());
                windowPosOrTimestamp.remove();
            }

            if (windowPosOrTimestamp.isEmpty()) {
                reset(context);
            }

            // skip values until they match the window
            while(currentInfluencerPosOrTimestamp < head) {
                currentInfluencerIndex++;

                if (ssaIterator.hasNext()) {
                    ssaIterator.next();
                    currentInfluencerPosOrTimestamp = ssaIterator.getValue();
                    currentInfluencerKey = ssaIterator.getKey();
                } else {
                    currentInfluencerPosOrTimestamp = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }

            // push matching values
            while(currentInfluencerPosOrTimestamp <= tail) {
                push(context, currentInfluencerKey, currentInfluencerIndex);
                windowKeys.add(currentInfluencerKey);
                windowPosOrTimestamp.add(currentInfluencerPosOrTimestamp);
                windowIndices.add(currentInfluencerIndex);
                currentInfluencerIndex++;

                if (ssaIterator.hasNext()) {
                    ssaIterator.next();
                    currentInfluencerPosOrTimestamp = ssaIterator.getValue();
                    currentInfluencerKey = ssaIterator.getKey();
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
     * @param timestampColumnName   the optional time stamp column for windowing (uses ticks if not provided)
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
        this.timestampColumnSource = timestampColumnSource == null ? null : ReinterpretUtils.maybeConvertToPrimitive(timestampColumnSource);
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.redirContext = redirContext;
    }

    public abstract void push(UpdateContext context, long key, int pos);
    public abstract void pop(UpdateContext context, long key, int pos);
    public abstract void reset(UpdateContext context);

    // return the first row that affects this key
    public long computeFirstAffectingKey(long key, @NotNull final RowSet source, final LongSegmentedSortedArray timestampSsa) {

        if (timestampColumnName == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos - reverseTimeScaleUnits : keyPos - reverseTimeScaleUnits + 1;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        } else {

        }
        return -1;
    }

    // return the last row that affects this key
    public long computeLastAffectingKey(long key, @NotNull final RowSet source, final LongSegmentedSortedArray timestampSsa) {
        if (timestampColumnName == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = keyPos + forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        } else {

        }
        return -1;
    }

    // return the first row affected by this key
    public long computeFirstAffectedKey(long key, @NotNull final RowSet source, final LongSegmentedSortedArray timestampSsa) {
        if (timestampColumnName == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos - forwardTimeScaleUnits - 1 : keyPos - forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        } else {
            // time-based, use the SSA
//            long ts = this.time
        }
        return -1;
    }

    // return the last row affected by this key
    public long computeLastAffectedKey(long key, @NotNull final RowSet source, final LongSegmentedSortedArray timestampSsa) {
        if (timestampColumnName == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos + reverseTimeScaleUnits - 1 : keyPos + reverseTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        } else {

        }
        return -1;
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
                              @NotNull final RowSet updateRowSet) {
        final UpdateWindowedContext ctx = (UpdateWindowedContext) context;

        // pre=load all the influencer values this update will need
        ctx.loadInfluencerValueChunk();

        // load all the influencer keys
        ctx.influencerKeyChunk = new SizedLongChunk<RowKeys>(ctx.influencerRows.intSize());
        ctx.influencerRows.fillRowKeyChunk(ctx.influencerKeyChunk.get());
        ctx.currentInfluencerKey = ctx.influencerRows.firstRowKey();

        if (timestampColumnName == null) {
            // load all the influencer positions
            ctx.influencerPosChunk = new SizedLongChunk<RowKeys>(ctx.influencerRows.intSize());
            ctx.influencerPositions.fillRowKeyChunk(ctx.influencerPosChunk.get());
            ctx.currentInfluencerPosOrTimestamp = ctx.influencerPositions.firstRowKey();
        } else {
            ctx.ssaIterator = ctx.timestampSsa.iterator(false, false);
            ctx.currentInfluencerPosOrTimestamp = ctx.ssaIterator.nextValue();
        }
        ctx.currentInfluencerIndex = 0;
    }

    @Override
    public void finishFor(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext)context;
        ctx.newModified = ctx.getModifiedBuilder().build();
    }

    @NotNull
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext)context;
        return ctx.newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext context) {
        UpdateWindowedContext ctx = (UpdateWindowedContext)context;
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
        return new String[] { pair.leftColumn };
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

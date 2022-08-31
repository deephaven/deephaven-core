package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.LongRingBuffer;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public abstract class UpdateByWindowedOperator implements UpdateByOperator {
    protected final OperationControl control;
    protected final LongRecordingUpdateByOperator recorder;
    protected final String timestampColumnName;
    protected final long reverseTimeScaleUnits;
    protected final long forwardTimeScaleUnits;

    protected final MatchPair pair;
    protected final String[] affectingColumns;

    protected UpdateBy.UpdateByRedirectionContext redirContext;

    public abstract class UpdateWindowedContext implements UpdateContext {
        public RowSetBuilderSequential modifiedBuilder;
        public RowSet newModified;

        public RowSetBuilderSequential getModifiedBuilder() {
            if(modifiedBuilder == null) {
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
        protected RowSet influencerKeys;
        protected long currentInfluencerKey;

        // candidate data for the window
        public final int WINDOW_CHUNK_SIZE = 4096;

        // persist two iterators, for the head and the tail of the current window
        protected RowSet.Iterator influencerPosIterator;
        protected RowSet.Iterator influencerKeyIterator;

        // for use with a ticking window
        protected RowSet affectedRowPositions;
        protected RowSet influencerPositions;
        protected long currentInfluencerPos;
        protected int currentInfluencerIndex;

        protected LongRingBuffer windowKeys = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        protected LongRingBuffer windowPos = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);
        protected LongRingBuffer windowIndices = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        /***
         * This function is only correct if the proper {@code source} rowset is provided.  If using buckets, then the
         * provided rowset must be limited to the rows in the current bucket
         * only
         *
         * @param upstream the update
         * @param source the rowset of the parent table (affected rows will be a subset)
         * @param upstreamAppendOnly
         */
        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source,
                                            final boolean upstreamAppendOnly) {

            // NOTE: this is fast rather than bounding to the smallest set possible. Will result in computing more than
            // actually necessary

            // TODO: return the minimal set of data for this update

            RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            if (upstream.removed().isNonempty()) {
                // need rows affected by removes in pre-shift space to determine rows to recompute
                try (final RowSet prevSource = source.copyPrev()) {

                    long s = computeFirstAffectedKey(upstream.removed().firstRowKey(), prevSource);
                    long e = computeLastAffectedKey(upstream.removed().lastRowKey(), prevSource);

                    try (final WritableRowSet tmp = RowSetFactory.fromRange(s,e)) {
                        // apply shifts to get this back to post-shift
                        upstream.shifted().apply(tmp);

                        builder.addRowSet(tmp);
                    }
                }
            }

            if (upstream.added().isNonempty()) {
                // all the new rows need computed
                builder.addRowSet(upstream.added());

                // add the rows affected by the adds
                builder.addRange(computeFirstAffectedKey(upstream.added().firstRowKey(), source),
                        computeLastAffectedKey(upstream.added().lastRowKey(), source));
            }

            if (upstream.modified().isNonempty()) {
                // add the rows affected by the mods
                builder.addRange(computeFirstAffectedKey(upstream.modified().firstRowKey(), source),
                        computeLastAffectedKey(upstream.modified().lastRowKey(), source));
            }

            try (final RowSet ignored = affectedRows;
                 final RowSet ignored2 = influencerKeys;
                 final RowSet ignored3 = affectedRowPositions;
                 final RowSet ignored4 = influencerPositions;
                 final RowSet brs = builder.build()) {

                affectedRows = source.intersect(brs);

                WritableRowSet tmp = RowSetFactory.fromRange(
                    computeFirstAffectingKey(affectedRows.firstRowKey(), source),
                    computeLastAffectingKey(affectedRows.lastRowKey(), source)
                );
                tmp.retain(source);
                influencerKeys = tmp;

                // generate position data rowsets for efficiently computed position offsets
                if (timestampColumnName == null) {
                    affectedRowPositions = source.invert(affectedRows);
                    influencerPositions = source.invert(influencerKeys);
                }
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
            while (!windowPos.isEmpty() && windowPos.front() < head) {
                pop(context, windowKeys.remove(), (int)windowIndices.remove());
                windowPos.remove();
            }

            // check our current values to see if it now matches the window
            while(currentInfluencerPos <= tail) {
                push(context, currentInfluencerKey, currentInfluencerIndex);
                windowKeys.add(currentInfluencerKey);
                windowPos.add(currentInfluencerPos);
                windowIndices.add(currentInfluencerIndex);
                currentInfluencerIndex++;

                if (influencerPosIterator.hasNext()) {
                    currentInfluencerKey = influencerKeyIterator.next();
                    currentInfluencerPos = influencerPosIterator.next();
                } else {
                    currentInfluencerPos = Long.MAX_VALUE;
                    currentInfluencerKey = Long.MAX_VALUE;
                }
            }

            if (windowPos.isEmpty()) {
                reset(context);
            }
        }

        @Override
        public void close() {
            try (final RowSet.Iterator ignoredIt1 = influencerPosIterator;
                 final RowSet.Iterator ignoredIt2 = influencerKeyIterator;
                 final RowSet ignoredRs1 = affectedRows;
                 final RowSet ignoredRs2 = influencerKeys;
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
     * @param timeRecorder   an optional recorder for a timestamp column.  If this is null, it will be assumed time is
     *                       measured in integer ticks.
     * @param redirContext the row redirection context to use for the operation
     */
    public UpdateByWindowedOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final OperationControl control,
                                    @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                    @Nullable final String timestampColumnName,
                                    final long reverseTimeScaleUnits,
                                    final long forwardTimeScaleUnits,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.control = control;
        this.recorder = timeRecorder;
        this.timestampColumnName = timestampColumnName;
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.redirContext = redirContext;
    }

    public abstract void push(UpdateContext context, long key, int pos);
    public abstract void pop(UpdateContext context, long key, int pos);
    public abstract void reset(UpdateContext context);

    // return the first row that affects this key
    public long computeFirstAffectingKey(long key, @NotNull final RowSet source) {

        if (recorder == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos - reverseTimeScaleUnits : keyPos - reverseTimeScaleUnits + 1;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        }
        return -1;
    }

    // return the last row that affects this key
    public long computeLastAffectingKey(long key, @NotNull final RowSet source) {
        if (recorder == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = keyPos + forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        }
        return -1;
    }

    // return the first row affected by this key
    public long computeFirstAffectedKey(long key, @NotNull final RowSet source) {
        if (recorder == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos - forwardTimeScaleUnits - 1 : keyPos - forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        }
        return -1;
    }

    // return the last row affected by this key
    public long computeLastAffectedKey(long key, @NotNull final RowSet source) {
        if (recorder == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = (keyPos < 0) ? -keyPos + reverseTimeScaleUnits - 1 : keyPos + reverseTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            } else if (idx < 0) {
                return source.firstRowKey();
            }
            return source.get(idx);
        }
        return -1;
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
                              @NotNull final RowSet updateRowSet) {
        final UpdateWindowedContext ctx = (UpdateWindowedContext) context;
        // load all the influencer values this update will need
        ctx.loadInfluencerValueChunk();

        // setup the iterators we will need for managing the windows
        ctx.influencerKeyIterator = ctx.influencerKeys.iterator();
        ctx.currentInfluencerKey = ctx.influencerKeyIterator.next();
        if (ctx.influencerPositions != null) {
            ctx.influencerPosIterator = ctx.influencerPositions.iterator();
            ctx.currentInfluencerPos = ctx.influencerPosIterator.next();
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
        return true;
    }

    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        // windowed operators don't need current values supplied to them, they only care about windowed values which
        // may or may not intersect with the column values
        return false;
    }
}

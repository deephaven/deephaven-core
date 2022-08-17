package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.base.LongRingBuffer;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.updateby.internal.LongRecordingUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;

public abstract class UpdateByWindowedOperator implements UpdateByOperator {
    protected final OperationControl control;
    protected final LongRecordingUpdateByOperator recorder;
    protected final long reverseTimeScaleUnits;
    protected final long forwardTimeScaleUnits;

    protected final MatchPair pair;
    protected final String[] affectingColumns;

    protected UpdateBy.UpdateByRedirectionContext redirContext;

    public abstract class UpdateWindowedContext implements UpdateContext {
        // store the current subset of rows that need computation
        protected RowSet affectedRows = RowSetFactory.empty();

        // candidate data for the window
        public final int WINDOW_CHUNK_SIZE = 1024;

        // data that is actually in the current window
        public LongRingBuffer windowRowKeys = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        // the selector that determines whether this value should be in the window, positions for tick-based and
        // timestamps for time-based operators
        public LongRingBuffer windowSelector = new LongRingBuffer(WINDOW_CHUNK_SIZE, true);

        public RowSequence.Iterator windowIterator = null;

        public RowSet workingRowSet = null;

        public WritableLongChunk<? extends RowKeys> candidateRowKeysChunk;
        public WritableLongChunk<? extends RowKeys> candidatePositionsChunk;
        public WritableLongChunk<Values> candidateTimestampsChunk;

        // position data for the chunk being currently processed
        public WritableLongChunk<? extends RowKeys> valuePositionChunk;

        public int candidateWindowIndex = 0;

        /***
         * This function is only correct if the proper {@code source} rowset is provided.  If using buckets, then the
         * provided rowset must be limited to the rows in the current bucket
         * only
         *
         * @param upstream the update
         * @param source the rowset of the parent table (affected rows will be a subset)
         * @param upstreamAppendOnly
         */
        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final RowSet source,
                                            final boolean upstreamAppendOnly) {

            // under certain circumstances, we can process directly and do not need to reprocess
            if (upstreamAppendOnly && forwardTimeScaleUnits <= 0 && reverseTimeScaleUnits >= 0) {
                try (final RowSet ignored = affectedRows) {
                    affectedRows = RowSetFactory.empty();
                }
                return affectedRows;
            }

            // NOTE: this is fast rather than bounding to the smallest set possible. Will result in computing more than
            // actually necessary

            // TODO: return the minimal set of data for this update

            RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            if (upstream.removed().isNonempty()) {
                // need removes in post-shift space to determine rows to recompute
                try (final WritableRowSet shiftedRemoves = upstream.removed().copy()) {
                    upstream.shifted().apply(shiftedRemoves);

                    builder.addRange(computeFirstAffectedKey(shiftedRemoves.firstRowKey(), source),
                            computeLastAffectedKey(shiftedRemoves.lastRowKey(), source));
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
                 final RowSet brs = builder.build()) {
                affectedRows = source.intersect(brs);
            }
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public abstract void loadCandidateValueChunk(RowSequence windowRowSequence);

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

            loadCandidateValueChunk(windowRowSequence);

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

                push(context, key, candidateWindowIndex);

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

        @Override
        public void close() {
            if (windowIterator != null) {
                windowIterator.close();
                windowIterator = null;
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

            // no need to close this, just release the reference
            workingRowSet = null;

            affectedRows.close();
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
                                    final long reverseTimeScaleUnits,
                                    final long forwardTimeScaleUnits,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.control = control;
        this.recorder = timeRecorder;
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.redirContext = redirContext;
    }

    public abstract void push(UpdateContext context, long key, int index);
    public abstract void pop(UpdateContext context, long key);
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
    public boolean requiresKeys() {
        return true;
    }
}

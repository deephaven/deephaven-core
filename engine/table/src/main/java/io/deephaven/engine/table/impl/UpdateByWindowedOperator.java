package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
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

    protected final boolean isRedirected;


    public abstract class UpdateWindowedContext implements UpdateContext {
        // store the current subset of rows that need computation
        protected RowSet affectedRows = RowSetFactory.empty();

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

            // NOTE: this is fast rather than bounding to the smallest set possible (i.e. unaware of buckets and
            // over-represents sparse data). Will result in computing far more than actually necessary

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
                // TODO: make this more efficient
                final List<String> cols = List.of(affectingColumns);
                boolean modifiedAffectingColumn = Arrays.stream(upstream.modifiedColumnSet().dirtyColumnNames()).anyMatch(cols::contains);

                if (modifiedAffectingColumn) {
                    // add the rows affected by the mods
                    builder.addRange(computeFirstAffectedKey(upstream.modified().firstRowKey(), source),
                            computeLastAffectedKey(upstream.modified().lastRowKey(), source));
                }
            }

            try (final RowSet ignored = affectedRows) {
                affectedRows = builder.build();
            }
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        @Override
        public void close() {
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
     * @param rowRedirection the row redirection to use for the operation
     */
    public UpdateByWindowedOperator(@NotNull final MatchPair pair,
                                    @NotNull final String[] affectingColumns,
                                    @NotNull final OperationControl control,
                                    @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                    final long reverseTimeScaleUnits,
                                    final long forwardTimeScaleUnits,
                                    @Nullable final RowRedirection rowRedirection) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.control = control;
        this.recorder = timeRecorder;
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.isRedirected = rowRedirection != null;
    }

    // return the first row that affects this key
    public long computeFirstAffectingKey(long key, @NotNull final RowSet source) {

        if (recorder == null) {
            // ticks
            final long keyPos = source.find(key);
            final long idx = keyPos - (long) reverseTimeScaleUnits + 1;
            if (idx < 0) {
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
            final long idx = keyPos + (long)forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
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
            final long idx = keyPos - (long) forwardTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
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
            final long idx = keyPos + (long) reverseTimeScaleUnits;
            if (idx >= source.size()) {
                return source.lastRowKey();
            }
            return source.get(idx);
        }
        return -1;
    }

    /***
     *  Windowed operators must always reprocess
     */
    @Override
    public boolean canProcessNormalUpdate(@NotNull UpdateContext context) {
        return false;
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

    @Override
    public void setBucketCapacity(int capacity) {
    }

    @Override
    public void onBucketsRemoved(@NotNull final RowSet removedBuckets) {
    }
}

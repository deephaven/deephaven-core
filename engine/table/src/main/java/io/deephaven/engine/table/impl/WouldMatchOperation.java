package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A table operation similar to {@link Table#where(String...)} except that instead of filtering the rows in the table,
 * it appends new columns containing the result of the filter evaluation on each row of the table. It will re-evaluate
 * cell values if any of the underlying filters are dynamic, and change.
 */
public class WouldMatchOperation implements QueryTable.MemoizableOperation<QueryTable> {
    private static final RowSet EMPTY_INDEX = RowSetFactory.empty();
    private final List<ColumnHolder> matchColumns;
    private final QueryTable parent;
    private QueryTable resultTable;
    private ModifiedColumnSet.Transformer transformer;

    /**
     * Just a little helper to keep column stuff together.
     */
    private static class ColumnHolder {
        final WouldMatchPair wouldMatchPair;
        IndexWrapperColumnSource column;

        ColumnHolder(WouldMatchPair pair) {
            this.wouldMatchPair = pair;
        }

        String getColumnName() {
            return wouldMatchPair.getColumnName();
        }

        WhereFilter getFilter() {
            return WhereFilter.of(wouldMatchPair.getFilter());
        }
    }

    WouldMatchOperation(QueryTable parent, WouldMatchPair... filters) {
        this.parent = parent;
        matchColumns = Arrays.stream(filters).map(ColumnHolder::new).collect(Collectors.toList());

        final List<String> parentColumns = parent.getDefinition().getColumnNames();

        final List<String> collidingColumns = matchColumns.stream()
                .map(ColumnHolder::getColumnName)
                .filter(parentColumns::contains)
                .collect(Collectors.toList());

        if (!collidingColumns.isEmpty()) {
            throw new UncheckedTableException(
                    "The table already contains the following columns: " + String.join(", ", collidingColumns));
        }
    }

    @Override
    public String getDescription() {
        return "match(" + makeDescription() + ")";
    }

    @Override
    public String getLogPrefix() {
        return "match";
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        MutableBoolean anyRefreshing = new MutableBoolean(false);

        try (final SafeCloseableList closer = new SafeCloseableList()) {
            final RowSet fullRowSet = usePrev ? closer.add(parent.getRowSet().copyPrev()) : parent.getRowSet();

            final List<NotificationQueue.Dependency> dependencies = new ArrayList<>();
            final Map<String, ColumnSource<?>> newColumns = new LinkedHashMap<>(parent.getColumnSourceMap());
            matchColumns.forEach(holder -> {
                final WhereFilter filter = holder.getFilter();
                filter.init(parent.getDefinition());
                final WritableRowSet result = filter.filter(fullRowSet, fullRowSet, parent, usePrev);
                holder.column = new IndexWrapperColumnSource(
                        holder.getColumnName(), parent, result.toTracking(), filter);

                if (newColumns.put(holder.getColumnName(), holder.column) != null) {
                    // This should never happen or the check in the constructor has failed.
                    throw new UncheckedTableException(
                            "In match(), column " + holder.getColumnName() + " already exists in the table.");
                }

                // Accumulate dependencies
                if (filter instanceof NotificationQueue.Dependency) {
                    dependencies.add((NotificationQueue.Dependency) filter);
                } else if (filter instanceof DependencyStreamProvider) {
                    ((DependencyStreamProvider) filter).getDependencyStream().forEach(dependencies::add);
                }

                if (filter.isRefreshing()) {
                    anyRefreshing.setTrue();
                }
            });

            this.resultTable = new QueryTable(parent.getRowSet(), newColumns);

            transformer =
                    parent.newModifiedColumnSetTransformer(resultTable, parent.getDefinition().getColumnNamesArray());

            // Set up the column to be a listener for recomputes
            matchColumns.forEach(mc -> {
                if (mc.getFilter() instanceof LivenessArtifact) {
                    resultTable.manage((LivenessArtifact) mc.getFilter());
                }
                mc.column.setResultTable(resultTable);
                mc.getFilter().setRecomputeListener(mc.column);
            });

            TableUpdateListener eventualListener = null;
            MergedListener eventualMergedListener = null;
            if (parent.isRefreshing()) {
                // If we're refreshing, our final listener needs to handle upstream updates from a recorder.
                final ListenerRecorder recorder =
                        new ListenerRecorder("where(" + makeDescription() + ")", parent, resultTable);
                final Listener listener = new Listener(recorder, dependencies);
                recorder.setMergedListener(listener);

                eventualMergedListener = listener;
                eventualListener = recorder;
            } else if (anyRefreshing.isTrue()) {
                // If not, then we still need to update if any of our filters request updates. We'll use the
                // merge listener to handle that. Note that the filters themselves should set the table to
                // refreshing.
                eventualMergedListener = new StaticListener(dependencies);
            }

            if (eventualMergedListener != null) {
                final MergedListener finalMergedListener = eventualMergedListener;
                resultTable.addParentReference(eventualMergedListener);
                matchColumns.forEach(h -> h.column.setMergedListener(finalMergedListener));
            }

            return new Result<>(resultTable, eventualListener);
        }
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.wouldMatch();
    }

    /**
     * A {@link MergedListener} implementation for {@link Table#wouldMatch(WouldMatchPair...)} when the parent table is
     * ticking.
     */
    private class Listener extends MergedListener {
        final ListenerRecorder recorder;

        Listener(@NotNull ListenerRecorder recorder,
                @NotNull List<NotificationQueue.Dependency> dependencies) {
            super(Collections.singletonList(recorder),
                    dependencies,
                    "merge(" + makeDescription() + ")",
                    resultTable);
            this.recorder = recorder;
        }

        @Override
        protected void process() {
            final TableUpdate downstream = new TableUpdateImpl(
                    recorder.getAdded().copy(),
                    recorder.getRemoved().copy(),
                    recorder.getModified().copy(),
                    recorder.getShifted(),
                    resultTable.modifiedColumnSet);

            transformer.clearAndTransform(recorder.getModifiedColumnSet(), downstream.modifiedColumnSet());

            // Propagate the updates to each column, inserting any additional modified rows post-shift that were
            // produced
            // by each column (ie. if a filter required a recompute
            matchColumns.stream()
                    .map(vc -> vc.column.update(recorder.getAdded(),
                            recorder.getRemoved(),
                            recorder.getModified(),
                            recorder.getModifiedPreShift(),
                            recorder.getShifted(),
                            recorder.getModifiedColumnSet(),
                            downstream.modifiedColumnSet(),
                            parent))
                    .filter(Objects::nonNull)
                    .forEach(rs -> {
                        downstream.modified().writableCast().insert(rs);
                        rs.close();
                    });

            resultTable.notifyListeners(downstream);
        }
    }

    /**
     * A {@link MergedListener} implementation for {@link Table#wouldMatch(WouldMatchPair...)} when * the parent table
     * is static (not ticking).
     */
    private class StaticListener extends MergedListener {
        StaticListener(@NotNull List<NotificationQueue.Dependency> dependencies) {
            super(Collections.emptyList(),
                    dependencies,
                    "wouldMatch(" + makeDescription() + ")",
                    resultTable);
        }

        @Override
        protected void process() {
            TableUpdate downstream = null;
            for (final ColumnHolder holder : matchColumns) {
                if (holder.column.recomputeRequested()) {
                    if (downstream == null) {
                        downstream =
                                new TableUpdateImpl(RowSetFactory.empty(),
                                        RowSetFactory.empty(),
                                        RowSetFactory.empty(),
                                        RowSetShiftData.EMPTY,
                                        resultTable.modifiedColumnSet);
                    }

                    downstream.modifiedColumnSet().setAll(holder.getColumnName());
                    try (final RowSet recomputed = holder.column.recompute(parent, EMPTY_INDEX)) {
                        downstream.modified().writableCast().insert(recomputed);
                    }
                }
            }

            if (downstream != null) {
                resultTable.notifyListeners(downstream);
            }
        }
    }

    private String makeDescription() {
        final StringBuilder sb = new StringBuilder();
        matchColumns.forEach(v -> {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append('[').append(v.getColumnName()).append('=').append(v.getFilter()).append(']');
        });

        return sb.toString();
    }

    private static class IndexWrapperColumnSource extends AbstractColumnSource<Boolean>
            implements MutableColumnSourceGetDefaults.ForBoolean, WhereFilter.RecomputeListener {
        private final TrackingWritableRowSet source;
        private final WhereFilter filter;
        private boolean doRecompute = false;
        private QueryTable resultTable;
        private MergedListener mergedListener;
        private final String name;
        private final ModifiedColumnSet possibleUpstreamModified;

        IndexWrapperColumnSource(String name, QueryTable parent, TrackingWritableRowSet sourceRowSet,
                WhereFilter filter) {
            super(Boolean.class);
            this.source = sourceRowSet;
            this.filter = filter;
            this.name = name;
            this.possibleUpstreamModified =
                    parent.newModifiedColumnSet(filter.getColumns().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        }

        @Override
        public Boolean get(long index) {
            return source.find(index) >= 0;
        }

        @Override
        public Boolean getPrev(long index) {
            return source.findPrev(index) >= 0;
        }

        @Override
        public void fillChunk(@NotNull FillContext context,
                @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            try (final RowSet keysToCheck = rowSequence.asRowSet();
                    final RowSet intersection = keysToCheck.intersect(source)) {
                fillChunkInternal(keysToCheck, intersection, rowSequence.intSize(), destination);
            }
        }

        @Override
        public void fillPrevChunk(@NotNull FillContext context,
                @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            try (final RowSet keysToCheck = rowSequence.asRowSet();
                    final RowSet sourcePrev = source.copyPrev();
                    final RowSet intersection = keysToCheck.intersect(sourcePrev)) {
                fillChunkInternal(keysToCheck, intersection, rowSequence.intSize(), destination);
            }
        }

        /**
         * Fill a chunk by walking the set of requested keys and the intersection of these keys together.
         *
         * @param keysToCheck the requested set of keys for the chunk
         * @param intersection the intersection of keys to check with the current source
         * @param RowSequenceSize the total number of keys requested
         * @param destination the destination chunk
         */
        private void fillChunkInternal(RowSet keysToCheck, RowSet intersection, int RowSequenceSize,
                @NotNull WritableChunk<? super Values> destination) {
            final WritableObjectChunk<Boolean, ? super Values> writeable =
                    destination.asWritableObjectChunk();
            writeable.setSize(RowSequenceSize);
            if (intersection.isEmpty()) {
                writeable.fillWithValue(0, RowSequenceSize, false);
                return;
            } else if (intersection.size() == RowSequenceSize) {
                writeable.fillWithValue(0, RowSequenceSize, true);
                return;
            }

            final RowSet.Iterator keysIterator = keysToCheck.iterator();
            final RowSet.Iterator intersectionIterator = intersection.iterator();

            long currentIntersectionKey = intersectionIterator.nextLong();

            int chunkIndex = 0;
            while (keysIterator.hasNext()) {
                final long inputKey = keysIterator.nextLong();
                final boolean equalsKey = inputKey == currentIntersectionKey;

                writeable.set(chunkIndex++, equalsKey);

                if (equalsKey) {
                    if (!intersectionIterator.hasNext()) {
                        break;
                    }
                    currentIntersectionKey = intersectionIterator.nextLong();
                }
            }

            // If this is true, we've bailed out of the lockstep iteration because there are no more intersections,
            // so we can fill the rest with false.
            if (keysIterator.hasNext()) {
                writeable.fillWithValue(chunkIndex, RowSequenceSize - chunkIndex, false);
            }

            destination.setSize(RowSequenceSize);
        }

        @Override
        public void requestRecompute() {
            doRecompute = true;
            Require.neqNull(mergedListener, "mergedListener").notifyChanges();
        }

        @Override
        public void requestRecomputeUnmatched() {
            // TODO: No need to recompute matched rows
            doRecompute = true;
            Require.neqNull(mergedListener, "mergedListener").notifyChanges();
        }

        @Override
        public void requestRecomputeMatched() {
            // TODO: No need to recompute unmatched rows
            doRecompute = true;
            Require.neqNull(mergedListener, "mergedListener").notifyChanges();
        }

        @NotNull
        @Override
        public QueryTable getTable() {
            return resultTable;
        }

        @Override
        public void setIsRefreshing(boolean refreshing) {
            resultTable.setRefreshing(refreshing);
        }

        /**
         * Update the internal RowSet with the upstream {@link TableUpdateImpl}. If the column was recomputed, return an
         * optional containing rows that were modified.
         *
         * @param added the set of added rows in the update
         * @param removed the set of removed rows in the update
         * @param modified the set of modified rows in the update
         * @param modPreShift the set of pre-shift modified rows in the update
         * @param shift the shifts included in the update
         * @param upstreamModified the modified parent columns
         * @param downstreamModified the modified set for the downstream notification
         * @param table the table to apply filters to
         *
         * @return an Optional containing rows modified to add to the downstream update
         */
        @Nullable
        private RowSet update(RowSet added, RowSet removed, RowSet modified,
                RowSet modPreShift, RowSetShiftData shift,
                ModifiedColumnSet upstreamModified, ModifiedColumnSet downstreamModified,
                QueryTable table) {
            final boolean affected = upstreamModified != null && upstreamModified.containsAny(possibleUpstreamModified);

            // Remove the removed keys, and pre-shift modifieds
            source.remove(removed);

            // If this column is affected, removed the modifieds pre-shift. We will refilter and add back
            // ones that match. If not, just leave them, the shift will preserve them.
            if (affected) {
                source.remove(modPreShift);
            }

            // Shift the RowSet
            shift.apply(source);

            if (doRecompute) {
                downstreamModified.setAll(name);
                return recompute(table, added);
            }

            try (final SafeCloseableList toClose = new SafeCloseableList()) {
                // Filter and add addeds
                final WritableRowSet filteredAdded = toClose.add(filter.filter(added, source, table, false));
                RowSet keysToRemove = EMPTY_INDEX;

                // If we were affected, recompute mods and re-add the ones that pass.
                if (affected) {
                    downstreamModified.setAll(name);
                    final RowSet filteredModified = toClose.add(filter.filter(modified, source, table, false));

                    // Now apply the additions and remove any non-matching modifieds
                    filteredAdded.insert(filteredModified);
                    keysToRemove = toClose.add(modified.minus(filteredModified));
                }

                source.update(filteredAdded, keysToRemove);
            }
            return null;
        }

        private RowSet recompute(QueryTable table, RowSet upstreamAdded) {
            doRecompute = false;
            final RowSet rowsChanged;
            try (final SafeCloseableList toClose = new SafeCloseableList()) {
                final WritableRowSet refiltered =
                        toClose.add(filter.filter(table.getRowSet().copy(), table.getRowSet(), table, false));

                // This is just Xor, but there is no RowSet op for that
                final RowSet newlySet = toClose.add(refiltered.minus(source));
                final RowSet justCleared = toClose.add(source.minus(refiltered));
                rowsChanged = toClose.add(newlySet.union(justCleared))
                        .minus(upstreamAdded);

                source.update(newlySet, justCleared);
            }
            return rowsChanged;
        }

        boolean recomputeRequested() {
            return doRecompute;
        }

        public void setResultTable(QueryTable resultTable) {
            this.resultTable = resultTable;
        }

        void setMergedListener(MergedListener listener) {
            this.mergedListener = listener;
        }
    }
}

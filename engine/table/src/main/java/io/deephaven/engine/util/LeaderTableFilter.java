//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.MultiSourceFunctionalColumn;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Return rows from multiple follower tables with the IDs indicated by the leader table.
 *
 * <p>
 * The Deephaven system does not provide cross table (or partition) transactions. However, you may have a producer that
 * generates more than one table, which you would like to then use in a coordinated fashion. The Deephaven system
 * preserves order within one partition, but the relative order of tables can not be guaranteed. Deephaven
 * infrastructure typically processes each partition independently to provide maximum throughput; and thus a row that
 * was created later may appear in your query before another row that was created earlier within a different partition.
 * </p>
 *
 * <p>
 * The LeaderTable filter provides a mechanism to write multiple tables with correlated ID values. One table is the
 * "leader" table and contains a row that indicates which rows from the "follower" tables should be considered together.
 * The leader and follower tables perform this calculation by key, which can be zero or more columns. The leader table
 * contains one column with a long ID per follower table which is matched to a long ID in that follower table.
 * </p>
 *
 * <p>
 * The leader and follower tables must be <i>add only</i>, they are not permitted to modify, shift or remove rows. If
 * you use a simple filter on an add-only source table, then they should remain add only.
 * </p>
 *
 * <p>
 * In this example, there is a SyncLog table that acts as the leader for the messageLog and trades table. The tables
 * must share keys in common to perform the synchronization. The columns must be the same type, though need not share
 * the same name.
 * </p>
 *
 * <p>
 * If we assume you have three add-only tables, named "syncLog", "tradeLog", and "messageLog" suitable for use with the
 * LeaderTableFilter, to create the Builder we pass in the leader table with the key columns specified, in this case
 * "client" and "session".
 * </p>
 *
 * <pre>
 * LeaderTableFilter.TableBuilder builder = new LeaderTableFilter.TableBuilder(syncLog, "client", "session");
 * </pre>
 *
 * Next, we add each of our follower tables. The third argument is a {@link io.deephaven.api.JoinMatch} in which the
 * leader table's column name is on the left side and the follower's column name is on the right side. In this case the
 * syncLog table has a "msgId" column and the messageLog simply has an "id" column. In our result, the table will be
 * named "messageLog". The key columns are "client" and "SessionId", which must match the "client" and "session" columns
 * in the leader table.
 *
 * <pre>
 * builder.addTable("messageLog", messageLog, "msgId=id", "client", "SessionId");
 * </pre>
 *
 * If the leader and follower tables have the same column name, then that single column name can be used instead of
 * duplicating the column name.
 *
 * <pre>
 * builder.addTable("trades", trades, "execId", "client", "SessionId");
 * </pre>
 *
 * After adding all the tables, then the build() method is called to create a {@link Results Results&lt;Table&gt;},
 * which implements a {@link Results#get(String)} method to retrieve result tables by name:
 *
 * <pre>
 * Results&lt;Table&gt; result = builder.build();
 *
 * Table filteredMessageLog = result.get("messageLog");
 * Table filteredTrades = result.get("trades");
 * </pre>
 *
 * The leader table is also filtered to indicate which IDs are active, and can be retrieved from the Result as well,
 * either by name or with the {@link Results#getLeader()}.
 *
 * <pre>
 * Table filteredLeader = result.get(io.deephaven.engine.util.LeaderTableFilter.DEFAULT_LEADER_NAME);
 * </pre>
 *
 * <p>
 * As an alternative to the TableBuilder you may use the {@link PartitionedTableBuilder} The PartitionedTableBuilder is
 * very similar to the TableBuilder, but takes {@link PartitionedTable PartitionedTables} as input and produces a
 * {@link Results} containing PartitionedTables as output instead of a {@link Results} containing Tables. Entries are
 * added to the result after all the input PartitionedTables have a matching key. The number of key columns for each
 * PartitionedTable must be identical, and of compatible types, the underlying tables of constituents are joined
 * together on the {@link PartitionedTable#keyColumnNames() key columns}, in order. Each table within the
 * PartitionedTable must be add-only.
 * </p>
 */
public class LeaderTableFilter {

    private static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("LeaderTableFilter.chunkSize", 1 << 16);
    private static final int DEFAULT_BINARY_SEARCH_THRESHOLD =
            Configuration.getInstance().getIntegerWithDefault("LeaderTableFilter.binarySearchThreshold", 1 << 16);
    static final String DEFAULT_LEADER_NAME = "LEADER";

    private final List<FollowerTableDescription> followerTables;

    private final QueryTable leaderResult;
    private final TrackingWritableRowSet leaderResultRowSet;
    private final ChunkType keyChunkType;

    private final QueryTable[] followerResults;
    private final TrackingWritableRowSet[] followerResultRowSets;

    private final TupleSource<?> leaderKeySource;
    private final ColumnSource<Long>[] followerIdsInLeaderSources;

    private final TupleSource<?>[] followerKeySources;
    private final ColumnSource<Long>[] followerIdSources;
    private final List<Map<Object, FollowerKeyState>> followerKeyStateMap;
    private final HashSet<Object> pendingKeys = new HashSet<>();
    // the leader is always at position 0, the followers are at positions 1...
    private final List<ListenerRecorder> recorders;

    private final Map<Object, LeaderKeyState> leaderKeyStateMap;

    // the bitmask that indicates all of our followers are satisfied
    private static final int MAX_FOLLOWERS = Integer.BYTES * 8;
    private final int completelySatisfiedMask;
    private final String leaderName;
    private final int binarySearchThreshold;

    private class LeaderKeyState {
        LeaderKeyState() {
            leaderRows = new TLongArrayList();
            pendingIdsByFollower = new TLongArrayList[followerKeySources.length];
            for (int ii = 0; ii < pendingIdsByFollower.length; ++ii) {
                pendingIdsByFollower[ii] = new TLongArrayList();
            }
            satisfiedIdsByFollower = new TIntArrayList();
        }

        // our currently matched row key in the leader table
        long matchedRowKey = RowSet.NULL_ROW_KEY;

        // an array list of pending rows in the leader that map to this state
        TLongArrayList leaderRows;
        // the corresponding ID that must be checked for each follower
        TLongArrayList[] pendingIdsByFollower;

        // a bitmask with one value set for each of our pending row sets; bit 0 is the first follower, bit 1 the second
        // follower, etc.
        TIntArrayList satisfiedIdsByFollower;

        private int size() {
            return leaderRows.size();
        }

        public void deleteHead(int matchedRowKey) {
            leaderRows.remove(0, matchedRowKey + 1);
            for (int tt = 0; tt < pendingIdsByFollower.length; ++tt) {
                pendingIdsByFollower[tt].remove(0, matchedRowKey + 1);
            }
            satisfiedIdsByFollower.remove(0, matchedRowKey + 1);
        }
    }

    private static class FollowerKeyState {
        WritableRowSet pendingRows = RowSetFactory.empty();
        WritableRowSet matchedRows = RowSetFactory.empty();
        RowSetBuilderSequential unprocessedBuilder = null;
        RowSetBuilderSequential currentIdBuilder = null;
        final TLongArrayList unprocessedIds = new TLongArrayList();
        boolean sorted = true;
        long activeId = Long.MIN_VALUE;
        long lastMatchedId = Long.MIN_VALUE;

        private void setActiveId(long activeId) {
            this.activeId = activeId;
        }

        private void compact() {
            if (sorted) {
                return;
            }
            unprocessedIds.sort();
            sorted = true;
            int wp = 1;
            for (int rp = 1; rp < unprocessedIds.size(); ++rp) {
                if (unprocessedIds.get(rp - 1) != unprocessedIds.get(rp)) {
                    unprocessedIds.set(wp++, unprocessedIds.get(rp));
                }
            }
            if (wp != unprocessedIds.size()) {
                unprocessedIds.remove(wp, unprocessedIds.size() - wp);
            }
        }

        public void deleteUnprocessedIds(int matchedFollowerIndex) {
            unprocessedIds.remove(0, matchedFollowerIndex + 1);
        }
    }

    private static abstract class FollowerDescription {
        final String name;
        final String followerIdColumn;
        final String leaderIdColumn;
        final String[] keyColumns;

        FollowerDescription(final String name, final String followerIdColumn, final String leaderIdColumn,
                final String[] keyColumns) {
            this.name = name;
            this.followerIdColumn = followerIdColumn;
            this.leaderIdColumn = leaderIdColumn;
            this.keyColumns = keyColumns;
        }
    }

    private static class FollowerTableDescription extends FollowerDescription {
        final Table table;

        FollowerTableDescription(final String name, final Table table, final String leaderIdColumn,
                final String followerIdColumn, final String[] keyColumns) {
            super(name, followerIdColumn, leaderIdColumn, keyColumns);
            this.table = table;
            if (!table.hasColumns(followerIdColumn)) {
                throw new IllegalArgumentException(
                        "Table \"" + name + "\" does not have ID column \"" + followerIdColumn + "\"");
            }
            if (!table.hasColumns(keyColumns)) {
                throw new IllegalArgumentException(
                        "Table \"" + name + "\" has missing key columns " + missingColumns(table, keyColumns));
            }
        }
    }

    private static class FollowerPartitionedTableDescription extends FollowerDescription {
        final PartitionedTable partitionedTable;

        FollowerPartitionedTableDescription(final String name, final PartitionedTable partitionedTable,
                final String leaderIdColumn, final String followerIdColumn, final String[] keyColumns) {
            super(name, followerIdColumn, leaderIdColumn, keyColumns);
            this.partitionedTable = partitionedTable;
        }
    }

    // must only be called through our builder
    private LeaderTableFilter(final Table rawLeaderTable,
            final String[] leaderKeys,
            final String leaderName,
            final List<FollowerTableDescription> followerTables,
            final int binarySearchThreshold) {
        if (followerTables.isEmpty()) {
            throw new IllegalArgumentException("No follower tables specified!");
        }

        final UpdateGraph leaderUpdateGraph =
                rawLeaderTable.getUpdateGraph(followerTables.stream().map(ftd -> ftd.table).toArray(Table[]::new));
        final boolean refreshing =
                rawLeaderTable.isRefreshing() || followerTables.stream().anyMatch(ftd -> ftd.table.isRefreshing());
        if (refreshing) {
            leaderUpdateGraph.checkInitiateSerialTableOperation();
        }

        final QueryTable leaderTable = (QueryTable) (rawLeaderTable.coalesce());
        if (!leaderTable.isAddOnly()) {
            throw new IllegalArgumentException("Leader table must be add only!");
        }

        this.leaderName = leaderName;
        this.followerTables = followerTables;
        this.binarySearchThreshold = binarySearchThreshold;

        final int tableCount = followerTables.size();
        if (tableCount > MAX_FOLLOWERS) {
            throw new IllegalArgumentException("Only " + MAX_FOLLOWERS + " follower tables are supported!");
        }
        this.completelySatisfiedMask = ((1 << tableCount) - 1);
        this.followerKeyStateMap = new ArrayList<>(tableCount);
        this.followerKeySources = new TupleSource[tableCount];
        // noinspection unchecked
        this.followerIdSources = new ColumnSource[tableCount];
        // noinspection unchecked
        this.followerIdsInLeaderSources = new ColumnSource[tableCount];

        this.followerResults = new QueryTable[tableCount];
        this.followerResultRowSets = new TrackingWritableRowSet[tableCount];
        this.recorders = new ArrayList<>(tableCount + 1);

        leaderResultRowSet = RowSetFactory.empty().toTracking();
        final ColumnSource<?>[] leaderSources =
                Arrays.stream(leaderKeys).map(leaderTable::getColumnSource).toArray(ColumnSource[]::new);
        leaderKeySource = TupleSourceFactory.makeTupleSource(leaderSources);
        keyChunkType = leaderKeySource.getChunkType();
        leaderKeyStateMap = new HashMap<>();
        createLeaderRecorder(leaderTable);

        final QueryTable[] coalescedFollowerTables = new QueryTable[tableCount];
        for (int ii = 0; ii < tableCount; ++ii) {
            final FollowerTableDescription ftd = followerTables.get(ii);
            final QueryTable followerTable = (QueryTable) ftd.table.coalesce();

            if (!followerTable.isAddOnly()) {
                throw new IllegalArgumentException(
                        "All follower tables must be add only! Table " + ftd.name + " is not add only.");
            }
            coalescedFollowerTables[ii] = followerTable;
            createFollowerRecorder(ftd);
        }

        final MergedLeaderListener mergedListener = refreshing ? new MergedLeaderListener() : null;
        if (refreshing) {
            recorders.forEach(lr -> {
                if (lr != null) {
                    lr.setMergedListener(mergedListener);
                }
            });
            leaderResult = leaderTable.getSubTable(leaderResultRowSet, null, null, mergedListener);
        } else {
            leaderResult = leaderTable.getSubTable(leaderResultRowSet, null, null);
        }

        for (int ii = 0; ii < tableCount; ++ii) {
            final FollowerTableDescription ftd = followerTables.get(ii);
            final QueryTable followerTable = coalescedFollowerTables[ii];
            final ColumnSource<?>[] sources =
                    Arrays.stream(ftd.keyColumns).map(followerTable::getColumnSource).toArray(ColumnSource[]::new);
            checkCompatibility(leaderSources, ftd, sources);
            followerKeyStateMap.add(new HashMap<>());
            followerKeySources[ii] = TupleSourceFactory.makeTupleSource(sources);
            followerIdSources[ii] = followerTable.getColumnSource(ftd.followerIdColumn, long.class);
            followerIdsInLeaderSources[ii] = leaderTable.getColumnSource(ftd.leaderIdColumn, long.class);
            followerResultRowSets[ii] = RowSetFactory.empty().toTracking();
            if (refreshing) {
                followerResults[ii] = followerTable.getSubTable(followerResultRowSets[ii], null, null, mergedListener);
            } else {
                followerResults[ii] = followerTable.getSubTable(followerResultRowSets[ii], null, null);
            }
            followerResults[ii].setLastNotificationStep(leaderUpdateGraph.clock().currentStep());
        }

        consumeLeaderRows(leaderTable.getRowSet());
        for (int ii = 0; ii < tableCount; ++ii) {
            consumeFollowerRows(ii, followerTables.get(ii).table.getRowSet());
        }

        final ProcessPendingResult processPendingResult = processPendingKeys();
        Assert.eqZero(processPendingResult.keysWithNewCurrent.size(), "hashSetPair.keysWithNewCurrent.size()");
        Assert.eqZero(processPendingResult.leaderRemoved.size(), "processPendingResult.leaderRemoved.size()");
        for (int tt = 0; tt < tableCount; tt++) {
            final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
            for (Object key : processPendingResult.keysToRefilter) {
                final FollowerKeyState state = followerKeyStateMap.get(tt).get(key);
                if (state != null) {
                    doMatch(tt, state);
                    addedBuilder.addRowSet(state.matchedRows);
                }
            }
            followerResultRowSets[tt].insert(addedBuilder.build());
        }
        leaderResultRowSet.insert(processPendingResult.leaderMatches);
        leaderResultRowSet.initializePreviousValue();
    }

    private static void checkCompatibility(ColumnSource<?>[] leaderSources, FollowerTableDescription ftd,
            ColumnSource<?>[] sources) {
        if (sources.length != leaderSources.length) {
            throw new IllegalArgumentException("Key sources are not compatible for " + ftd.name + " ("
                    + (typeString(sources)) + ") and leader (" + (typeString(leaderSources)) + ")");
        }
        for (int cc = 0; cc < sources.length; cc++) {
            if (leaderSources[cc].getChunkType() != sources[cc].getChunkType()) {
                throw new IllegalArgumentException("Key sources are not compatible for " + ftd.name + " ("
                        + (typeString(sources)) + ") and leader (" + (typeString(leaderSources)) + ")");
            }
        }
    }

    private void createFollowerRecorder(FollowerTableDescription ftd) {
        if (!ftd.table.isRefreshing()) {
            recorders.add(null);
            return;
        }
        final ListenerRecorder listenerRecorder =
                new ListenerRecorder("LeaderTableFilter(" + ftd.name + ")", ftd.table, null);
        ftd.table.addUpdateListener(listenerRecorder);
        recorders.add(listenerRecorder);
    }

    private void createLeaderRecorder(QueryTable leader) {
        if (!leader.isRefreshing()) {
            recorders.add(null);
            return;
        }
        final ListenerRecorder listenerRecorder =
                new ListenerRecorder("LeaderTableFilter(" + leaderName + ")", leader, null);
        leader.addUpdateListener(listenerRecorder);
        recorders.add(listenerRecorder);
    }

    class MergedLeaderListener extends MergedListener {
        MergedLeaderListener() {
            super(recorders.stream().filter(Objects::nonNull).collect(Collectors.toList()), Collections.emptyList(),
                    "MergedLeaderListener", null);
        }

        @Override
        protected void process() {
            final ListenerRecorder leaderRecorder = recorders.get(0);
            if (leaderRecorder != null && leaderRecorder.recordedVariablesAreValid()) {
                consumeLeaderRows(leaderRecorder.getAdded());
            }

            for (int rr = 1; rr < recorders.size(); ++rr) {
                final ListenerRecorder recorder = recorders.get(rr);
                if (recorder != null && recorder.recordedVariablesAreValid()) {
                    // we are valid
                    consumeFollowerRows(rr - 1, recorder.getAdded());
                }
            }

            final ProcessPendingResult processPendingResult = processPendingKeys();
            for (int tt = 0; tt < followerKeyStateMap.size(); tt++) {
                final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
                final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
                for (final Object key : processPendingResult.keysToRefilter) {
                    final FollowerKeyState state = followerKeyStateMap.get(tt).get(key);
                    if (state == null) {
                        // we have never seen anything for this key on this table, which means that we must have a
                        // NULL_LONG identifier in the leader table.
                        continue;
                    }
                    final boolean removeMatches = state.lastMatchedId != state.activeId;
                    final RowSet lastMatched;
                    if (removeMatches) {
                        removedBuilder.addRowSet(state.matchedRows);
                        lastMatched = null;
                    } else {
                        lastMatched = state.matchedRows.copy();
                    }
                    doMatch(tt, state);
                    if (removeMatches) {
                        addedBuilder.addRowSet(state.matchedRows);
                    } else {
                        try (final RowSet ignored = lastMatched;
                                final RowSet newlyMatched = state.matchedRows.minus(lastMatched)) {
                            addedBuilder.addRowSet(newlyMatched);
                        }
                    }
                }

                for (final Object key : processPendingResult.keysWithNewCurrent) {
                    final FollowerKeyState state = followerKeyStateMap.get(tt).get(key);
                    if (state == null || state.currentIdBuilder == null) {
                        continue;
                    }
                    if (!processPendingResult.keysToRefilter.contains(key)) {
                        // if we did not refilter this key; then we should add the currently matched values,
                        // otherwise we ignore them because they have already been superseded
                        final WritableRowSet newlyMatchedRows = state.currentIdBuilder.build();
                        state.matchedRows.insert(newlyMatchedRows);
                        newlyMatchedRows.remove(followerResultRowSets[tt]);
                        addedBuilder.addRowSet(newlyMatchedRows);
                    }
                    state.currentIdBuilder = null;
                }

                final RowSet removed = removedBuilder.build();
                final RowSet added = addedBuilder.build();
                followerResultRowSets[tt].remove(removed);
                followerResultRowSets[tt].insert(added);

                Assert.assertion(!added.overlaps(removed), "!added.overlaps(removed)", added, "added", removed,
                        "removed");

                if (added.isNonempty() || removed.isNonempty()) {
                    final TableUpdateImpl update = new TableUpdateImpl();
                    update.added = added;
                    update.removed = removed;
                    update.modified = RowSetFactory.empty();
                    update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    update.shifted = RowSetShiftData.EMPTY;
                    followerResults[tt].notifyListeners(update);
                }
            }

            if (processPendingResult.leaderMatches.isNonempty() || processPendingResult.leaderRemoved.isNonempty()) {
                leaderResultRowSet.update(processPendingResult.leaderMatches, processPendingResult.leaderRemoved);
                leaderResult.notifyListeners(processPendingResult.leaderMatches, processPendingResult.leaderRemoved,
                        RowSetFactory.empty());
            }
        }

        @Override
        protected void propagateErrorDownstream(boolean fromProcess, @NotNull Throwable error,
                TableListener.@Nullable Entry entry) {
            final List<BaseTable> deferred = new ArrayList<>();

            Stream.concat(Stream.of(leaderResult), Stream.of(followerResults)).forEach(result -> {
                if (fromProcess && result.satisfied(getUpdateGraph().clock().currentStep())) {
                    // If the result is already satisfied (because it managed to send its notification, or was otherwise
                    // satisfied) we should not send our error notification on this cycle.
                    if (!result.isFailed()) {
                        // If the result isn't failed, we need to mark it as such on the next cycle.
                        deferred.add(result);
                    }
                } else {
                    result.notifyListenersOnError(error, entry);
                }
            });

            if (!deferred.isEmpty()) {
                scheduleDelayedErrorNotifier(error, entry, deferred);
            }
        }

        @Override
        public boolean systemicResult() {
            return SystemicObjectTracker.isSystemic(leaderResult)
                    || Arrays.stream(followerResults).anyMatch(SystemicObjectTracker::isSystemic);
        }
    }

    private void doMatch(final int tableIndex, final FollowerKeyState state) {
        final long matchValue = state.activeId;

        final boolean addMatches = state.lastMatchedId == matchValue;
        state.lastMatchedId = matchValue;

        if (matchValue == QueryConstants.NULL_LONG) {
            // when our active value is NULL, no rows should be returned from this table
            state.matchedRows = RowSetFactory.empty();
            return;
        }

        final RowSetBuilderSequential matchedBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential pendingBuilder = RowSetFactory.builderSequential();

        if (state.pendingRows.size() > binarySearchThreshold) {
            // Binary search within the pending rows to eliminate values that are earlier than our matchValue.
            // We could also binary search at the end of the segment to determine the exact set of matchValues, but
            // the initialization case is more likely to have a lot of rows at the "front" which are old, and no
            // longer relevant. A third possibility would be to have the state contain a map from ID to matching
            // row keys (instead of just the pending rows), and we could then directly match the rows without the
            // need to reread them at all at the cost of maintaining more row keys.
            long firstPositionInclusive = 0;
            long lastPositionExclusive = state.pendingRows.size();
            while (firstPositionInclusive < lastPositionExclusive - 1) {
                final long midPoint = (firstPositionInclusive + lastPositionExclusive) / 2;
                final long midPointIndex = state.pendingRows.get(midPoint);
                final long midPointId = followerIdSources[tableIndex].getLong(midPointIndex);
                if (midPointId >= matchValue) {
                    // if we are equal to (or bigger than) the matchValue, we do not want to include it in the range of
                    // rows we will delete from the pendingRows
                    lastPositionExclusive = midPoint;
                } else {
                    // we are less than the matchValue, so should be excluded from the pendingRows
                    firstPositionInclusive = midPoint;
                }
            }
            state.pendingRows.removeRange(0, firstPositionInclusive);
        }

        final int pendingChunkSize = (int) Math.min(state.pendingRows.size(), CHUNK_SIZE);

        try (final WritableLongChunk<OrderedRowKeys> rowKeysChunk =
                WritableLongChunk.makeWritableChunk(pendingChunkSize);
                final RowSequence.Iterator rsit = state.pendingRows.getRowSequenceIterator();
                final ColumnSource.GetContext getContext =
                        followerIdSources[tableIndex].makeGetContext(pendingChunkSize)) {
            while (rsit.hasMore()) {
                final RowSequence chunkRs = rsit.getNextRowSequenceWithLength(pendingChunkSize);
                chunkRs.fillRowKeyChunk(rowKeysChunk);
                final LongChunk<? extends Values> idChunk =
                        followerIdSources[tableIndex].getChunk(getContext, chunkRs).asLongChunk();
                for (int ii = 0; ii < idChunk.size(); ++ii) {
                    final long id = idChunk.get(ii);
                    if (id > matchValue) {
                        pendingBuilder.appendKey(rowKeysChunk.get(ii));
                    } else if (id == matchValue) {
                        matchedBuilder.appendKey(rowKeysChunk.get(ii));
                    }
                }
            }
        }

        state.pendingRows = pendingBuilder.build();
        if (addMatches) {
            try (final RowSet newMatches = matchedBuilder.build()) {
                state.matchedRows.insert(newMatches);
            }
            if (state.currentIdBuilder != null) {
                try (final RowSet curentMatches = state.currentIdBuilder.build()) {
                    state.matchedRows.insert(curentMatches);
                    state.currentIdBuilder = null;
                }
            }
        } else {
            state.matchedRows = matchedBuilder.build();
        }
    }

    private static class ProcessPendingResult {
        final HashSet<Object> keysToRefilter;
        final HashSet<Object> keysWithNewCurrent;
        final RowSet leaderMatches;
        final RowSet leaderRemoved;

        private ProcessPendingResult(HashSet<Object> keysToRefilter, HashSet<Object> keysWithNewCurrent,
                RowSet leaderMatches, RowSet leaderRemoved) {
            this.keysToRefilter = keysToRefilter;
            this.keysWithNewCurrent = keysWithNewCurrent;
            this.leaderMatches = leaderMatches;
            this.leaderRemoved = leaderRemoved;
        }
    }

    @NotNull
    private ProcessPendingResult processPendingKeys() {
        final int tableCount = followerKeyStateMap.size();
        final HashSet<Object> keysToRefilter = new HashSet<>();
        final HashSet<Object> keysWithNewCurrent = new HashSet<>();
        final RowSetBuilderRandom leaderMatchBuilder = RowSetFactory.builderRandom();
        final RowSetBuilderRandom leaderRemovedBuilder = RowSetFactory.builderRandom();
        for (Object pendingKey : pendingKeys) {
            final FollowerKeyState[] followerKeyStates = new FollowerKeyState[tableCount];

            // before the early short circuit for the leader state check, we must flush the sequential builders created
            // on this cycle to the pendingRows to avoid out-of-order keys on the next iteration.
            for (int tt = 0; tt < tableCount; ++tt) {
                final FollowerKeyState followerKeyState =
                        followerKeyStates[tt] = followerKeyStateMap.get(tt).get(pendingKey);
                if (followerKeyState == null) {
                    continue;
                }
                if (followerKeyState.unprocessedBuilder != null) {
                    followerKeyState.pendingRows.insert(followerKeyState.unprocessedBuilder.build());
                    followerKeyState.unprocessedBuilder = null;
                }
            }

            final LeaderKeyState leaderKeyState = leaderKeyStateMap.get(pendingKey);
            if (leaderKeyState == null) {
                // we can never have any rows if our leader does not have a row for this key
                continue;
            }


            for (int tt = 0; tt < tableCount; ++tt) {
                final FollowerKeyState followerKeyState = followerKeyStates[tt];
                if (followerKeyState == null) {
                    continue;
                }
                if (followerKeyState.currentIdBuilder != null) {
                    keysWithNewCurrent.add(pendingKey);
                }
                followerKeyState.compact();
            }

            int[] pendingFollowerIndex = makePendingFollowers(tableCount);

            // for each unprocessed set of rows in the leader table, we should check the state of the follower table
            final int pendingLeaderRows = leaderKeyState.size();
            int matchedIndex = -1;
            for (int leaderRowToCheck = pendingLeaderRows - 1; leaderRowToCheck >= 0; leaderRowToCheck--) {
                int satisfiedFollowers = leaderKeyState.satisfiedIdsByFollower.get(leaderRowToCheck);
                for (int tt = 0; tt < tableCount; ++tt) {
                    final int tableMask = 1 << tt;
                    if ((satisfiedFollowers & tableMask) != 0) {
                        continue;
                    }
                    // we need to check if this follower has the key for this row
                    final long idForFollower = leaderKeyState.pendingIdsByFollower[tt].get(leaderRowToCheck);
                    if (idForFollower == QueryConstants.NULL_LONG) {
                        satisfiedFollowers |= tableMask;
                        continue;
                    }

                    final FollowerKeyState followerKeyState = followerKeyStates[tt];
                    if (followerKeyState == null) {
                        // the follower has never seen the key, so we are not satisfied
                        continue;
                    }
                    if (idForFollower == followerKeyState.activeId) {
                        // we are already active, so are satisfied and need not do any work
                        satisfiedFollowers |= tableMask;
                        continue;
                    }

                    // we must check the pending ids in the follower
                    final int foundIndex = followerKeyState.unprocessedIds.binarySearch(idForFollower);
                    if (foundIndex < 0) {
                        // not found
                        continue;
                    } else {
                        satisfiedFollowers |= tableMask;
                    }
                    pendingFollowerIndex[tt] = foundIndex;
                }
                if (satisfiedFollowers == completelySatisfiedMask) {
                    // we are happy with this result, set the active states for each of the tables; because we are
                    // scanning the leader table backwards, we want to take the most recent leader row
                    matchedIndex = leaderRowToCheck;
                    for (int tt = 0; tt < tableCount; ++tt) {
                        // the followerKeyState may be null if the pending Id is null
                        final long activeId = leaderKeyState.pendingIdsByFollower[tt].get(leaderRowToCheck);
                        if (followerKeyStates[tt] != null) {
                            followerKeyStates[tt].setActiveId(activeId);
                        } else {
                            Assert.eq(activeId, "activeId", QueryConstants.NULL_LONG);
                        }
                    }
                    break;
                } else {
                    // We are not yet ready; but an earlier row may be. We record what followers have already been
                    // satisfied so that we do not need to recheck them on the next pass
                    leaderKeyState.satisfiedIdsByFollower.set(leaderRowToCheck, satisfiedFollowers);
                }
            }

            if (matchedIndex >= 0) {
                final long newMatch = leaderKeyState.leaderRows.get(matchedIndex);
                leaderMatchBuilder.addKey(newMatch);
                if (leaderKeyState.matchedRowKey != RowSet.NULL_ROW_KEY) {
                    leaderRemovedBuilder.addKey(leaderKeyState.matchedRowKey);
                }
                leaderKeyState.matchedRowKey = newMatch;
                leaderKeyState.deleteHead(matchedIndex);
                for (int tt = 0; tt < tableCount; ++tt) {
                    if (followerKeyStates[tt] != null) {
                        followerKeyStates[tt].deleteUnprocessedIds(pendingFollowerIndex[tt]);
                    }
                }
                keysToRefilter.add(pendingKey);
            }
        }
        pendingKeys.clear();
        return new ProcessPendingResult(keysToRefilter, keysWithNewCurrent, leaderMatchBuilder.build(),
                leaderRemovedBuilder.build());
    }

    @NotNull
    private static int[] makePendingFollowers(int tableCount) {
        int[] pendingFollowerIndex = new int[tableCount];
        Arrays.fill(pendingFollowerIndex, -1);
        return pendingFollowerIndex;
    }

    private void consumeLeaderRows(final RowSet rowset) {
        final ChunkSource.GetContext[] idGetContexts = new ChunkSource.GetContext[followerIdsInLeaderSources.length];
        // noinspection unchecked
        final LongChunk<? extends Values>[] followerIdsInLeaderChunks =
                new LongChunk[followerIdsInLeaderSources.length];

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowset.size());

        try (SharedContext sharedContext = SharedContext.makeSharedContext();
                final WritableChunk<Values> primitiveLeaderKeyChunk = keyChunkType.makeWritableChunk(chunkSize);
                final RowSequence.Iterator rsit = rowset.getRowSequenceIterator();
                final ChunkSource.FillContext keyFillContext =
                        leaderKeySource.makeFillContext(chunkSize, sharedContext);
                final WritableLongChunk<OrderedRowKeys> rowKeysChunk =
                        WritableLongChunk.makeWritableChunk(chunkSize);
                final ChunkBoxer.BoxerKernel boxerKernel = ChunkBoxer.getBoxer(keyChunkType, chunkSize);
                final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(idGetContexts)) {
            for (int cc = 0; cc < followerIdsInLeaderSources.length; ++cc) {
                idGetContexts[cc] = followerIdsInLeaderSources[cc].makeGetContext(chunkSize, sharedContext);
            }

            while (rsit.hasMore()) {
                final RowSequence chunkRs = rsit.getNextRowSequenceWithLength(chunkSize);
                chunkRs.fillRowKeyChunk(rowKeysChunk);

                leaderKeySource.fillChunk(keyFillContext, primitiveLeaderKeyChunk, chunkRs);
                final ObjectChunk<?, ? extends Values> leaderKeyChunk = boxerKernel.box(primitiveLeaderKeyChunk);

                for (int cc = 0; cc < followerIdsInLeaderSources.length; ++cc) {
                    followerIdsInLeaderChunks[cc] =
                            followerIdsInLeaderSources[cc].getChunk(idGetContexts[cc], chunkRs).asLongChunk();
                }

                // for each row insert a value into our LeaderKeyState
                for (int ii = 0; ii < leaderKeyChunk.size(); ++ii) {
                    final Object rowKey = leaderKeyChunk.get(ii);
                    final LeaderKeyState lks = leaderKeyStateMap.computeIfAbsent(rowKey, k -> new LeaderKeyState());

                    lks.leaderRows.add(rowKeysChunk.get(ii));
                    for (int cc = 0; cc < followerIdsInLeaderChunks.length; ++cc) {
                        lks.pendingIdsByFollower[cc].add(followerIdsInLeaderChunks[cc].get(ii));
                    }
                    lks.satisfiedIdsByFollower.add(0);

                    pendingKeys.add(rowKey);
                }

                sharedContext.reset();
            }
        }
    }

    private void consumeFollowerRows(final int tableIndex, final RowSet rowSet) {
        final ColumnSource<Long> idSource = followerIdSources[tableIndex];
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowSet.size());
        try (final WritableChunk<Values> primitiveFollowerKeyValuesChunk = keyChunkType.makeWritableChunk(chunkSize);
                final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                final ChunkSource.FillContext idFillContext = idSource.makeFillContext(chunkSize);
                final ChunkSource.FillContext keyFillContext =
                        followerKeySources[tableIndex].makeFillContext(chunkSize);
                final ChunkBoxer.BoxerKernel boxerKernel = ChunkBoxer.getBoxer(keyChunkType, chunkSize);
                final WritableLongChunk<Values> idChunk = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> rowKeysChunk =
                        WritableLongChunk.makeWritableChunk(chunkSize)) {
            while (rsit.hasMore()) {
                final RowSequence chunkRs = rsit.getNextRowSequenceWithLength(chunkSize);
                chunkRs.fillRowKeyChunk(rowKeysChunk);

                followerKeySources[tableIndex].fillChunk(keyFillContext, primitiveFollowerKeyValuesChunk, chunkRs);
                final ObjectChunk<?, ? extends Values> followerKeyValuesChunk =
                        boxerKernel.box(primitiveFollowerKeyValuesChunk);

                idSource.fillChunk(idFillContext, idChunk, chunkRs);

                // We are potentially looking each value up in the hash table multiple times, if we were to sort
                // the data we could avoid that. Of course, if we chose that strategy, we would pay the cost of a
                // sort instead.
                Object lastKey = null;
                FollowerKeyState lastState = null;
                for (int ii = 0; ii < idChunk.size(); ++ii) {
                    final FollowerKeyState currentState;

                    final Object key = followerKeyValuesChunk.get(ii);
                    if (key == lastKey) {
                        currentState = lastState;
                    } else {
                        pendingKeys.add(key);
                        currentState =
                                followerKeyStateMap.get(tableIndex).computeIfAbsent(key, (k) -> new FollowerKeyState());
                        lastKey = key;
                        lastState = currentState;
                    }

                    final long activeId = currentState.activeId;

                    final long id = idChunk.get(ii);
                    if (id == QueryConstants.NULL_LONG || (id < activeId)) {
                        continue;
                    }
                    if (id == activeId) {
                        if (currentState.currentIdBuilder == null) {
                            currentState.currentIdBuilder = RowSetFactory.builderSequential();
                        }
                        currentState.currentIdBuilder.appendKey(rowKeysChunk.get(ii));
                        continue;
                    }

                    if (currentState.unprocessedBuilder == null) {
                        currentState.unprocessedBuilder = RowSetFactory.builderSequential();
                    }

                    currentState.unprocessedBuilder.appendKey(rowKeysChunk.get(ii));
                    final int unprocessedCount = currentState.unprocessedIds.size();
                    if (unprocessedCount == 0) {
                        currentState.unprocessedIds.add(id);
                    } else {
                        final long lastUnprocessed = currentState.unprocessedIds.get(unprocessedCount - 1);
                        if (lastUnprocessed != id) {
                            currentState.unprocessedIds.add(id);
                            if (lastUnprocessed >= id) {
                                currentState.sorted = false;
                            }
                        }
                    }
                }
            }
        }
    }

    private static String typeString(final ColumnSource<?>... sources) {
        return Arrays.stream(sources).map(cs -> cs.getType().getSimpleName()).collect(Collectors.joining(", "));
    }

    /**
     * Produce a Result containing the leader and follower Tables.
     *
     * @return a result containing the filtered Tables
     */
    private Results<Table> getResult() {
        final Table[] results = new Table[followerResults.length + 1];
        final String[] names = new String[followerResults.length + 1];

        results[0] = leaderResult;
        names[0] = leaderName;

        for (int ii = 0; ii < followerResults.length; ++ii) {
            results[ii + 1] = followerResults[ii];
            names[ii + 1] = followerTables.get(ii).name;
        }

        return new ResultsImpl<>(results, names);
    }

    private static List<String> missingColumns(Table table, String... expected) {
        return Arrays.stream(expected).filter(ex -> !table.getDefinition().getColumnNames().contains(ex))
                .collect(Collectors.toList());
    }

    /**
     * Produce a Result of synchronized tables.
     */
    public static class TableBuilder {
        private String leaderName = DEFAULT_LEADER_NAME;

        private int binarySearchThreshold = DEFAULT_BINARY_SEARCH_THRESHOLD;

        @NotNull
        private final String[] leaderKeys;

        @NotNull
        private final Table leaderTable;

        private final List<FollowerTableDescription> followerTables = new ArrayList<>();


        /**
         * Create a builder with a default ID and key columns.
         *
         * @param leaderTable the leader table
         * @param leaderKeys the name of the key columns in the leader table, also used as the default keys for follower
         *        tables
         */
        public TableBuilder(@NotNull final Table leaderTable, @NotNull final String... leaderKeys) {
            this.leaderTable = leaderTable;
            this.leaderKeys = leaderKeys;
            if (!leaderTable.hasColumns(leaderKeys)) {
                throw new IllegalArgumentException(
                        "Leader table does not have all key columns " + missingColumns(leaderTable, leaderKeys));
            }
        }

        /**
         * Set the name of the leader table in the returned Result.
         *
         * @param leaderName the name of the leader table in the returned Result
         */
        public TableBuilder setLeaderName(final String leaderName) {
            if (followerTables.stream().anyMatch(ftd -> leaderName.equals(ftd.name))) {
                throw new IllegalArgumentException("Follower table already has name " + leaderName);
            }
            this.leaderName = leaderName;
            return this;
        }

        /**
         * Set the minimum size of a pending key state that will result in a binary search instead of a linear scan for
         * rows that are earlier than the currently matched id.
         *
         * @param binarySearchThreshold the minimum number of pending rows (per key) that result in binary search)
         */
        public TableBuilder setBinarySearchThreshold(final int binarySearchThreshold) {
            this.binarySearchThreshold = binarySearchThreshold;
            return this;
        }

        /**
         * Add a table to the set of tables to be synchronized, using this builder's default key column names.
         *
         * @param name the key of the Table in our output Result of Tables.
         * @param table the Table to add
         * @param idColumn The name of the ID column in the Table, must be a long. Expressed as
         *        "LeaderName=FollowerName", or "ColumnName" when the names are the same.
         * @return this builder
         */
        public TableBuilder addTable(final String name, final Table table, final String idColumn) {
            return addTable(name, table, idColumn, leaderKeys);
        }

        /**
         * Add a table to the set of tables to be synchronized.
         *
         * @param name the key of the Table in our output Result of Tables.
         * @param table the Table to add
         * @param idColumn The name of the ID column in the Table, must be a long. Expressed as
         *        "LeaderName=FollowerName", or "ColumnName" when the names are the same.
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public TableBuilder addTable(final String name, final Table table, final String idColumn,
                final String... keyColumns) {
            return addTable(name, table, MatchPairFactory.getExpression(idColumn), keyColumns);
        }

        /**
         * Add a table to the set of tables to be synchronized.
         *
         * @param name the key of the Table in our output Result of Tables.
         * @param table the Table to add
         * @param idColumn a Pair with the leader ID column in the output and the follower as the input. The type of the
         *        columns must be long.
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public TableBuilder addTable(final String name, final Table table, final JoinMatch idColumn,
                final String... keyColumns) {
            final String leaderIdColumn = idColumn.left().name();
            final String followerIdColumn = idColumn.right().name();

            if (leaderName.equals(name)) {
                throw new IllegalArgumentException("Conflict with leader name \"" + name + "\"");
            }
            if (followerTables.stream().anyMatch(ftd -> ftd.name.equals(name))) {
                throw new IllegalArgumentException("Duplicate follower table name \"" + name + "\"");
            }
            if (!leaderTable.hasColumns(leaderIdColumn)) {
                throw new IllegalArgumentException("Leader table does not have ID column " + leaderIdColumn);
            }
            followerTables.add(new FollowerTableDescription(name, table, leaderIdColumn, followerIdColumn, keyColumns));
            return this;
        }

        /**
         * Instantiate the Result with synchronized tables.
         * <p>
         * This must be called under the UpdateGraph lock.
         *
         * @return a Result with one entry for each input Table (leader and all followers)
         */
        public Results<Table> build() {
            return QueryPerformanceRecorder.withNugget("LeaderTableFilter", () -> {
                if (followerTables.isEmpty()) {
                    throw new IllegalArgumentException(
                            "You must specify follower tables as parameters to the LeaderTableFilter.Builder");
                }
                return new LeaderTableFilter(leaderTable, leaderKeys, leaderName, followerTables,
                        binarySearchThreshold).getResult();
            });
        }
    }

    /**
     * Produce a Result of synchronized PartitionedTables.
     */
    public static class PartitionedTableBuilder {
        private String leaderName = DEFAULT_LEADER_NAME;
        private int binarySearchThreshold = DEFAULT_BINARY_SEARCH_THRESHOLD;
        private final String[] leaderKeys;

        private final PartitionedTable leaderPartitionedTable;

        private final List<FollowerPartitionedTableDescription> followerPartitionedTables = new ArrayList<>();

        /**
         * Create a builder with a default ID and key columns.
         *
         * @param leaderPartitionedTable the partitioned table of leader tables
         * @param leaderKeys the name of the key columns in the leader tables, also used as the default keys for
         *        follower tables
         */
        public PartitionedTableBuilder(final PartitionedTable leaderPartitionedTable, final String... leaderKeys) {
            this.leaderPartitionedTable = leaderPartitionedTable;
            this.leaderKeys = leaderKeys;
        }

        /**
         * Set the name of the leader PartitionedTable in the returned Result.
         *
         * @param leaderName the name of the leader PartitionedTable in the returned Result
         */
        public PartitionedTableBuilder setLeaderName(final String leaderName) {
            if (followerPartitionedTables.stream().anyMatch(ftd -> leaderName.equals(ftd.name))) {
                throw new IllegalArgumentException("Follower PartitionedTable already has name " + leaderName);
            }
            this.leaderName = leaderName;
            return this;
        }

        /**
         * Set the minimum size of a pending key state that will result in a binary search instead of a linear scan for
         * rows that are earlier than the currently matched id.
         *
         * @param binarySearchThreshold the minimum number of pending rows (per key) that result in binary search)
         */
        public PartitionedTableBuilder setBinarySearchThreshold(final int binarySearchThreshold) {
            this.binarySearchThreshold = binarySearchThreshold;
            return this;
        }

        /**
         * Add a PartitionedTable to the set of PartitionedTables to be synchronized, using this builder's default key
         * column names.
         *
         * @param name the key of the PartitionedTable in our Result
         * @param partitionedTable the PartitionedTable to add
         * @param idColumn The name of the ID column in the PartitionedTable, must be a long. Expressed as
         *        "LeaderName=FollowerName", or "ColumnName" when the names are the same.
         * @return this builder
         */
        public PartitionedTableBuilder addPartitionedTable(final String name, final PartitionedTable partitionedTable,
                final String idColumn) {
            return addPartitionedTable(name, partitionedTable, idColumn, leaderKeys);
        }

        /**
         * Add a PartitionedTable to the set of PartitionedTables to be synchronized.
         *
         * @param name the key of the PartitionedTable in our output PartitionedTable.
         * @param partitionedTable the PartitionedTable to add
         * @param idColumn The name of the ID column in the PartitionedTable, must be a long. Expressed as
         *        "LeaderName=FollowerName", or "ColumnName" when the names are the same.
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public PartitionedTableBuilder addPartitionedTable(final String name, final PartitionedTable partitionedTable,
                final String idColumn, final String... keyColumns) {
            return addPartitionedTable(name, partitionedTable, MatchPairFactory.getExpression(idColumn), keyColumns);
        }

        /**
         * Add a PartitionedTable to the set of PartitionedTables to be synchronized.
         *
         * @param name the key of the PartitionedTable in our output PartitionedTable.
         * @param partitionedTable the PartitionedTable to add
         * @param idColumn a JoinMatch with the leader ID column as the left, and the follower column as the right. The
         *        type of the columns must be long.
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public PartitionedTableBuilder addPartitionedTable(final String name, final PartitionedTable partitionedTable,
                final JoinMatch idColumn, final String... keyColumns) {
            final String leaderIdColumn = idColumn.left().name();
            final String followerIdColumn = idColumn.right().name();

            if (leaderName.equals(name)) {
                throw new IllegalArgumentException("Conflict with leader name \"" + name + "\"");
            }
            if (followerPartitionedTables.stream().anyMatch(ftd -> ftd.name.equals(name))) {
                throw new IllegalArgumentException("Duplicate follower PartitionedTable name \"" + name + "\"");
            }
            followerPartitionedTables.add(new FollowerPartitionedTableDescription(name, partitionedTable,
                    leaderIdColumn, followerIdColumn, keyColumns));
            return this;
        }

        /**
         * Instantiate the Result with synchronized PartitionedTables.
         * <p>
         * This must be called under the UpdateGraph lock.
         *
         * @return a Result with one entry for each input PartitionedTable
         */
        public Results<PartitionedTable> build() {
            return QueryPerformanceRecorder.withNugget("LeaderTableFilter", () -> {
                if (!followerPartitionedTables.isEmpty()) {
                    return createPartitionedTableAdapter(leaderPartitionedTable, leaderKeys, leaderName,
                            binarySearchThreshold,
                            followerPartitionedTables);
                } else {
                    throw new IllegalArgumentException(
                            "You must specify follower tables as parameters to the LeaderTableFilter.Builder");
                }
            });
        }

        private Results<PartitionedTable> createPartitionedTableAdapter(
                PartitionedTable leaderPartitionedTable,
                String[] leaderKeys,
                String leaderName,
                int binarySearchThreshold,
                List<FollowerPartitionedTableDescription> followerPartitionedTables) {

            if (!leaderPartitionedTable.uniqueKeys()) {
                throw new IllegalArgumentException("Leader must have unique partitioned table keys");
            }

            final List<String> sourceNames = new ArrayList<>();
            sourceNames.add(leaderPartitionedTable.constituentColumnName());

            final MultiJoinInput[] multiJoinInputs = new MultiJoinInput[followerPartitionedTables.size() + 1];

            multiJoinInputs[0] = MultiJoinInput.of(leaderPartitionedTable.table(),
                    leaderPartitionedTable.keyColumnNames().toArray(String[]::new));

            // we've got the partitioned tables, we need to join them all together
            for (int fi = 0; fi < followerPartitionedTables.size(); ++fi) {
                final FollowerPartitionedTableDescription fptd = followerPartitionedTables.get(fi);

                if (!fptd.partitionedTable.uniqueKeys()) {
                    throw new IllegalArgumentException(
                            "Follower " + fptd.name + " must have unique partitioned table keys");
                }

                final Set<String> followerKeyColumns = fptd.partitionedTable.keyColumnNames();
                final Set<String> leaderKeyColumns = leaderPartitionedTable.keyColumnNames();

                if (followerKeyColumns.size() != leaderKeyColumns.size()) {
                    throw new IllegalArgumentException(
                            "Follower PartitionedTable " + fptd.name + " has different keys (" + followerKeyColumns
                                    + ") than leader PartitionedTable (" + leaderKeyColumns + ")");
                }

                final List<JoinMatch> joinMatches = new ArrayList<>();
                final Iterator<String> lkc = leaderKeyColumns.iterator();
                for (final Iterator<String> fkc = followerKeyColumns.iterator(); fkc.hasNext();) {
                    final String followerKey = fkc.next();
                    final String leaderKey = lkc.next();
                    joinMatches.add(JoinMatch.of(ColumnName.of(leaderKey), ColumnName.of(followerKey)));
                }

                final String followerName = "__FOLLOWER_" + fi;
                sourceNames.add(followerName);

                final List<JoinAddition> columnsToAdd = List.of(JoinAddition.of(ColumnName.of(followerName),
                        ColumnName.of(fptd.partitionedTable.constituentColumnName())));

                multiJoinInputs[fi + 1] = MultiJoinInput.of(fptd.partitionedTable.table(), joinMatches, columnsToAdd);
            }

            final Table joinedConstituents = MultiJoinFactory.of(multiJoinInputs).table();

            // now that they are joined together, we want to listen to the result and when everything in a row becomes
            // non-null; then we should create a new LeaderTableFilter instance for those tables, and stick it into the
            // partitionedTables
            String resultName = "__RESULT_HOLDER";
            final Table withLtf = joinedConstituents.update(List.of(
                    new MultiSourceFunctionalColumn<>(sourceNames, resultName, Results.class,
                            (k, s) -> {
                                final Table leader = (Table) s[0].get(k);
                                if (leader == null) {
                                    return null;
                                }
                                final List<FollowerTableDescription> followers = new ArrayList<>();
                                for (int ii = 1; ii < s.length; ++ii) {
                                    Object follower = s[ii].get(k);
                                    if (follower == null) {
                                        return null;
                                    }
                                    FollowerPartitionedTableDescription fptd = followerPartitionedTables.get(ii - 1);
                                    followers.add(new FollowerTableDescription(fptd.name, (Table) follower,
                                            fptd.leaderIdColumn, fptd.followerIdColumn, fptd.keyColumns));
                                }

                                return new LeaderTableFilter(leader, leaderKeys, leaderName, followers,
                                        binarySearchThreshold).getResult();
                            })));

            final Table withResults = withLtf.where(Filter.isNotNull(ColumnName.of(resultName)));

            final PartitionedTable[] partitionedTables = new PartitionedTable[followerPartitionedTables.size() + 1];
            final String[] names = new String[followerPartitionedTables.size() + 1];

            final Table withLeaderResult = withResults.update(List.of(new FunctionalColumn<>(resultName,
                    Results.class, "__LEADER_RESULT", Table.class, r -> (Table) r.get(leaderName))));
            final PartitionedTable leaderResult =
                    new PartitionedTableImpl(withLeaderResult,
                            leaderPartitionedTable.keyColumnNames(), true, "__LEADER_RESULT",
                            leaderPartitionedTable.constituentDefinition(), true, false);
            names[0] = leaderName;
            partitionedTables[0] = leaderResult;

            for (int fi = 0; fi < followerPartitionedTables.size(); ++fi) {
                final PartitionedTable fpt = followerPartitionedTables.get(fi).partitionedTable;
                final String followerName = followerPartitionedTables.get(fi).name;
                final String followerResultColumn = "__FOLLOWER_" + fi + "_RESULT";
                final PartitionedTable followerResult =
                        new PartitionedTableImpl(
                                withResults.update(
                                        List.of(new FunctionalColumn<>(resultName, Results.class,
                                                followerResultColumn, Table.class, r -> (Table) r.get(followerName)))),
                                fpt.keyColumnNames(), true,
                                followerResultColumn, fpt.constituentDefinition(), true, false);
                partitionedTables[fi + 1] = followerResult;
                names[fi + 1] = followerName;
            }

            return new ResultsImpl<>(partitionedTables, names);
        }
    }


    /**
     * The Result class for a LeaderTableFilter, either containing {@link Table tables} or {@link PartitionedTable
     * partitioned tables}.
     */
    public interface Results<T> extends LivenessNode {
        /**
         * How many results objects are available? Equal to one (for the leader) plus the number of followers.
         * 
         * @return the number of results
         */
        int size();

        Set<String> keySet();

        /**
         * Call get to retrieve one result (either the leader or a follower), with the name specified in the builder.
         *
         * <p>
         * The result of the get call is not additionally managed, so the liveness of this result must be maintained for
         * the returned table to be valid.
         * </p>
         */
        T get(String name);

        /**
         * <p>
         * The result of the get call is not additionally managed, so the liveness of this result must be maintained for
         * the returned table to be valid.
         * </p>
         *
         * @return the leader result
         */
        T getLeader();
    }

    private static class ResultsImpl<T extends LivenessReferent> extends LivenessArtifact implements Results<T> {
        final String[] names;
        private final T[] results;

        public ResultsImpl(final T[] results, final String[] names) {
            this.names = names;
            this.results = results;
            for (final T tab : results) {
                manage(tab);
            }
        }

        @Override
        public T get(String name) {
            for (int ii = 0; ii < names.length; ++ii) {
                if (name.equals(names[ii])) {
                    return results[ii];
                }
            }
            throw new IllegalArgumentException(
                    "Table " + name + " not found, available results are " + Arrays.toString(names));
        }

        @Override
        public T getLeader() {
            return results[0];
        }

        @Override
        public int size() {
            return names.length;
        }

        @Override
        public Set<String> keySet() {
            return Set.of(names);
        }
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Return the rows with the highest commonly available ID from multiple tables.
 *
 * The Deephaven system does not provide cross table (or partition) transactions. However, you may have a producer that
 * generates more than one table, which you would like to then use in a coordinated fashion. The Deephaven system
 * preserves order within one partition, but the relative order of tables can not be guaranteed. The tailers, DIS, and
 * other infrastructure process each partition independently to provide maximum throughput; and thus a row that logged
 * later may appear in your query before another row that was logged earlier within a different partition.
 *
 * If you tag each row with a long column that can be used to correlate the two tables, the SyncTableFilter can release
 * only rows with matching values in all the input tables. For example, if you have input tables "a" "b" and "c", with a
 * key of "USym", if "a" has rows for SPY with IDs of 1, 2, and 3, but "b" and "c" only have rows for "2", the filter
 * will pass through the rows with ID 2. When both "b" and "c" have SPY rows for 3, then the rows for ID 2 are removed
 * and the rows for ID 3 are added to the result tables.
 *
 * The SyncTableFilter is configured through the Builder inner class. Tables are added to the builder, providing a name
 * for each table. The return value is a map of the results with each filtered input table accessible according to the
 * name provided to the builder.
 *
 * For each key, only pass through the rows that have the minimum value of an ID across all tables. The IDs must be
 * monotonically increasing for each key. Your underlying tables must make use of transactions such that all rows for a
 * given ID appear in a input table at once. Please consult your Deephaven representative before deploying a query that
 * includes this filter to ensure that the assumptions are not violated.
 */
public class SyncTableFilter {

    private static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("SyncTableFilter.chunkSize", 1 << 16);
    private final List<SyncTableDescription> tables;
    private final QueryTable[] results;
    private final TrackingWritableRowSet[] resultRowSet;

    private final TupleSource[] keySources;
    private final List<ColumnSource<Long>> idSources;
    private final List<Map<Object, KeyState>> objectToState;
    private final TObjectLongMap<Object> minimumid;
    private final HashSet<Object> pendingKeys = new HashSet<>();
    private final List<ListenerRecorder> recorders;

    private static class KeyState {
        WritableRowSet pendingRows = RowSetFactory.empty();
        WritableRowSet matchedRows = RowSetFactory.empty();
        RowSetBuilderSequential unprocessedBuilder = null;
        RowSetBuilderSequential currentIdBuilder = null;
        final TLongArrayList unprocessedIds = new TLongArrayList();
        int sortStart = -1;
    }

    private static class SyncTableDescription {

        final Table table;
        final String name;
        final String idColumn;
        final String[] keyColumns;

        SyncTableDescription(final String name, final Table table, final String idColumn, final String[] keyColumns) {
            this.name = name;
            this.idColumn = idColumn;
            this.keyColumns = keyColumns;
            this.table = table;
            if (!table.hasColumns(idColumn)) {
                throw new IllegalArgumentException(
                        "Table \"" + name + "\" does not have ID column \"" + idColumn + "\"");
            }
            if (!table.hasColumns(keyColumns)) {
                throw new IllegalArgumentException(
                        "Table \"" + name + "\" does not have key columns \"" + Arrays.toString(keyColumns) + "\"");
            }
        }
    }

    private SyncTableFilter(final List<SyncTableDescription> tables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("No tables specified!");
        }

        if (tables.stream().anyMatch(t -> t.table.isRefreshing())) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }

        // through the builder only
        this.tables = tables;
        final int tableCount = tables.size();
        this.objectToState = new ArrayList<>(tableCount);
        this.keySources = new TupleSource[tableCount];
        this.idSources = new ArrayList<>(tableCount);
        this.results = new QueryTable[tableCount];
        this.resultRowSet = new TrackingWritableRowSet[tableCount];
        this.minimumid = new TObjectLongHashMap<>(0, 0.5f, QueryConstants.NULL_LONG);
        this.recorders = new ArrayList<>(tableCount);

        ColumnSource[] keySourcePrototype = null;
        final MergedListener mergedListener = new MergedSyncListener(null);

        for (int ii = 0; ii < tableCount; ++ii) {
            final SyncTableDescription std = tables.get(ii);
            final ColumnSource[] sources =
                    Arrays.stream(std.keyColumns).map(std.table::getColumnSource).toArray(ColumnSource[]::new);
            if (ii == 0) {
                keySourcePrototype = sources;
            } else {
                if (sources.length != keySourcePrototype.length) {
                    throw new IllegalArgumentException(
                            "Key sources are not compatible for " + std.name + " (" + (typeString(sources)) + ") and "
                                    + tables.get(0).name + "(" + (typeString(keySourcePrototype)) + ")");
                }
                for (int cc = 0; cc < sources.length; cc++) {
                    if (keySourcePrototype[cc].getChunkType() != sources[cc].getChunkType()) {
                        throw new IllegalArgumentException(
                                "Key sources are not compatible for " + std.name + " (" + (typeString(sources))
                                        + ") and " + tables.get(0).name + "(" + (typeString(keySourcePrototype)) + ")");
                    }
                }
            }
            objectToState.add(new HashMap<>());
            keySources[ii] = TupleSourceFactory.makeTupleSource(sources);
            idSources.add(std.table.getColumnSource(std.idColumn, long.class));
            // noinspection resource
            resultRowSet[ii] = RowSetFactory.empty().toTracking();
            results[ii] = ((QueryTable) std.table).getSubTable(resultRowSet[ii], null, null, mergedListener);

            final ListenerRecorder listenerRecorder =
                    new ListenerRecorder("SyncTableFilter(" + std.name + ")", std.table, results[ii]);
            std.table.addUpdateListener(listenerRecorder);
            recorders.add(listenerRecorder);

            consumeRows(ii, std.table.getRowSet());
        }

        recorders.forEach(lr -> lr.setMergedListener(mergedListener));

        final Pair<HashSet<Object>, HashSet<Object>> hashSetPair = processPendingKeys();
        final HashSet<Object> keysToRefilter = hashSetPair.first;
        Assert.eqZero(hashSetPair.second.size(), "hashSetPair.second.size()");
        for (int tt = 0; tt < tableCount; tt++) {
            final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
            for (Object key : keysToRefilter) {
                final KeyState state = objectToState.get(tt).get(key);
                doMatch(tt, state, minimumid.get(key));
                addedBuilder.addRowSet(state.matchedRows);
            }
            resultRowSet[tt].insert(addedBuilder.build());
        }
        keysToRefilter.clear();
    }

    class MergedSyncListener extends io.deephaven.engine.table.impl.MergedListener {
        MergedSyncListener(final QueryTable dummyResult) {
            super(recorders, Collections.emptyList(), "SyncTableListener", dummyResult);
        }

        @Override
        protected void process() {
            final long currentStep = LogicalClock.DEFAULT.currentStep();

            for (int rr = 0; rr < recorders.size(); ++rr) {
                final ListenerRecorder recorder = recorders.get(rr);
                if (recorder.getNotificationStep() == currentStep) {
                    // we are valid
                    if (recorder.getRemoved().isNonempty()) {
                        throw new IllegalStateException("Can not process removed rows in SyncTableFilter!");
                    }
                    if (recorder.getShifted().nonempty()) {
                        throw new IllegalStateException("Can not process shifted rows in SyncTableFilter!");
                    }
                    final RowSet addedAndModified = recorder.getAdded().union(recorder.getModified());
                    consumeRows(rr, addedAndModified);
                }
            }

            final Pair<HashSet<Object>, HashSet<Object>> hashSetPair = processPendingKeys();
            final HashSet<Object> keysToRefilter = hashSetPair.first;
            final HashSet<Object> keysWithNewCurrentRows = hashSetPair.second;
            for (int tt = 0; tt < objectToState.size(); tt++) {
                final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
                final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
                for (Object key : keysToRefilter) {
                    final KeyState state = objectToState.get(tt).get(key);
                    removedBuilder.addRowSet(state.matchedRows);
                    doMatch(tt, state, minimumid.get(key));
                    addedBuilder.addRowSet(state.matchedRows);
                }

                for (Object key : keysWithNewCurrentRows) {
                    final KeyState state = objectToState.get(tt).get(key);
                    if (state.currentIdBuilder == null) {
                        continue;
                    }
                    if (!keysToRefilter.contains(key)) {
                        // if we did not refilter this key; then we should add the currently matched values,
                        // otherwise we ignore them because they have already been superseded
                        final WritableRowSet newlyMatchedRows = state.currentIdBuilder.build();
                        state.matchedRows.insert(newlyMatchedRows);
                        newlyMatchedRows.remove(resultRowSet[tt]);
                        addedBuilder.addRowSet(newlyMatchedRows);
                    }
                    state.currentIdBuilder = null;
                }

                final RowSet removed = removedBuilder.build();
                final RowSet added = addedBuilder.build();
                resultRowSet[tt].remove(removed);
                resultRowSet[tt].insert(added);

                final WritableRowSet addedAndRemoved = added.intersect(removed);

                final WritableRowSet modified;
                if (recorders.get(tt).getNotificationStep() == currentStep) {
                    modified = recorders.get(tt).getModified().intersect(resultRowSet[tt]);
                    modified.remove(added);
                    modified.insert(addedAndRemoved);
                } else {
                    modified = addedAndRemoved;
                }

                if (added.isNonempty() || removed.isNonempty() || modified.isNonempty()) {
                    results[tt].notifyListeners(added, removed, modified);
                }
            }
            keysToRefilter.clear();
        }

        @Override
        protected void propagateErrorDownstream(
                @NotNull final Throwable error, @Nullable final TableListener.Entry entry) {
            for (QueryTable result : results) {
                result.notifyListenersOnError(error, entry);
            }
        }

        @Override
        protected boolean systemicResult() {
            return Arrays.stream(results).anyMatch(SystemicObjectTracker::isSystemic);
        }
    }

    private void doMatch(final int tableIndex, final KeyState state, final long matchValue) {
        final RowSetBuilderSequential matchedBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential pendingBuilder = RowSetFactory.builderSequential();

        try (final WritableLongChunk<OrderedRowKeys> keyIndices =
                WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
                final RowSequence.Iterator rsIt = state.pendingRows.getRowSequenceIterator();
                final ColumnSource.GetContext getContext = idSources.get(tableIndex).makeGetContext(CHUNK_SIZE)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                chunkOk.fillRowKeyChunk(keyIndices);
                final LongChunk<? extends Values> idChunk =
                        idSources.get(tableIndex).getChunk(getContext, chunkOk).asLongChunk();
                for (int ii = 0; ii < idChunk.size(); ++ii) {
                    final long id = idChunk.get(ii);
                    if (id > matchValue) {
                        pendingBuilder.appendKey(keyIndices.get(ii));
                    } else if (id == matchValue) {
                        matchedBuilder.appendKey(keyIndices.get(ii));
                    }
                }
            }
        }

        state.pendingRows = pendingBuilder.build();
        state.matchedRows = matchedBuilder.build();
    }

    @NotNull
    private Pair<HashSet<Object>, HashSet<Object>> processPendingKeys() {
        final int tableCount = objectToState.size();
        final HashSet<Object> keysToRefilter = new HashSet<>();
        final HashSet<Object> keysWithNewCurrent = new HashSet<>();
        for (Object pendingKey : pendingKeys) {
            final KeyState[] keyStates = new KeyState[tableCount];
            boolean allPresent = true;
            for (int tt = 0; tt < tableCount; ++tt) {
                final KeyState keyState = keyStates[tt] = objectToState.get(tt).get(pendingKey);
                if (keyState == null) {
                    allPresent = false;
                    continue;
                }
                if (keyState.unprocessedBuilder != null) {
                    keyState.pendingRows.insert(keyState.unprocessedBuilder.build());
                    keyState.unprocessedBuilder = null;
                }
                if (keyState.currentIdBuilder != null) {
                    keysWithNewCurrent.add(pendingKey);
                }
                if (keyState.sortStart > -1) {
                    final TLongArrayList unprocessedIds = keyState.unprocessedIds;
                    unprocessedIds.sort();
                    keyState.sortStart = -1;
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
                if (keyState.unprocessedIds.size() == 0) {
                    allPresent = false;
                }
            }
            if (allPresent) {
                final int[] checkRps = new int[tableCount - 1];
                for (int ii = 1; ii < tableCount; ++ii) {
                    checkRps[ii - 1] = keyStates[ii].unprocessedIds.size() - 1;
                }

                int rp = keyStates[0].unprocessedIds.size() - 1;
                while (rp >= 0) {
                    final long maxUnprocessed = keyStates[0].unprocessedIds.get(rp);
                    boolean foundInAllTables = true;
                    for (int tt = 1; tt < tableCount; ++tt) {
                        while (checkRps[tt - 1] >= 0
                                && keyStates[tt].unprocessedIds.get(checkRps[tt - 1]) > maxUnprocessed) {
                            checkRps[tt - 1]--;
                        }
                        if (checkRps[tt - 1] < 0
                                || keyStates[tt].unprocessedIds.get(checkRps[tt - 1]) != maxUnprocessed) {
                            foundInAllTables = false;
                            break;
                        }
                    }
                    if (!foundInAllTables) {
                        rp--;
                    } else {
                        // this is the ID, yay, we'll need to refilter the pending rows in each table
                        minimumid.put(pendingKey, maxUnprocessed);
                        keysToRefilter.add(pendingKey);
                        // we need to clear out unprocessed IDS <= maxUnprocessed
                        keyStates[0].unprocessedIds.remove(0, rp + 1);
                        for (int tt = 1; tt < tableCount; ++tt) {
                            keyStates[tt].unprocessedIds.remove(0, checkRps[tt - 1] + 1);
                        }
                        break;
                    }
                }
            }
        }
        pendingKeys.clear();
        return new Pair<>(keysToRefilter, keysWithNewCurrent);
    }

    private void consumeRows(final int tableIndex, final RowSet rowSet) {
        final ColumnSource<Long> idSource = idSources.get(tableIndex);
        // TODO(1606): migrate to using TupleSource.fillChunk
        try (final WritableObjectChunk<Object, Any> valuesChunk =
                WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
                final WritableLongChunk<Values> idChunk = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
                final WritableLongChunk<OrderedRowKeys> keyIndicesChunk =
                        WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
                final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();
                final ColumnSource.FillContext fillContext = idSource.makeFillContext(CHUNK_SIZE)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                chunkOk.fillRowKeyChunk(keyIndicesChunk);
                valuesChunk.setSize(0);
                chunkOk.forEachRowKey(idx -> {
                    Object tuple = keySources[tableIndex].createTuple(idx);
                    valuesChunk.add(tuple);
                    return true;
                });
                idSource.fillChunk(fillContext, idChunk, chunkOk);

                // TODO(1606): we should sort this so we do not need to repeatedly look things up
                for (int ii = 0; ii < idChunk.size(); ++ii) {
                    final Object key = valuesChunk.get(ii);
                    pendingKeys.add(key);
                    final KeyState currentState =
                            objectToState.get(tableIndex).computeIfAbsent(key, (k) -> new KeyState());
                    if (currentState.unprocessedBuilder == null) {
                        currentState.unprocessedBuilder = RowSetFactory.builderSequential();
                    }
                    final long minid = minimumid.get(key);

                    final long id = idChunk.get(ii);
                    if (id == QueryConstants.NULL_LONG || (id < minid)) {
                        continue;
                    }
                    if (id == minid) {
                        if (currentState.currentIdBuilder == null) {
                            currentState.currentIdBuilder = RowSetFactory.builderSequential();
                        }
                        currentState.currentIdBuilder.appendKey(keyIndicesChunk.get(ii));
                        continue;
                    }
                    currentState.unprocessedBuilder.appendKey(keyIndicesChunk.get(ii));
                    final int unprocessedCount = currentState.unprocessedIds.size();
                    if (unprocessedCount == 0) {
                        currentState.unprocessedIds.add(id);
                    } else {
                        final long lastUnprocessed = currentState.unprocessedIds.get(unprocessedCount - 1);
                        if (lastUnprocessed != id) {
                            if (lastUnprocessed < id) {
                                currentState.unprocessedIds.add(id);
                            } else {
                                currentState.sortStart = currentState.unprocessedIds.size();
                                currentState.unprocessedIds.add(id);
                            }
                        }
                    }
                }
            }
        }
    }

    private static String typeString(final ColumnSource[] sources) {
        return Arrays.stream(sources).map(cs -> cs.getType().getSimpleName()).collect(Collectors.joining(", "));
    }

    private Map<String, Table> getMap() {
        final int size = tables.size();
        final Map<String, Table> map = new LinkedHashMap<>(size);
        for (int ii = 0; ii < size; ii++) {
            map.put(tables.get(ii).name, results[ii]);
        }
        return map;
    }

    /**
     * Produce a map of synchronized tables, keyed by the name assigned to each input table.
     */
    public static class Builder {
        private String defaultId;
        private String[] defaultKeys;
        private final List<SyncTableDescription> tables = new ArrayList<>();

        /**
         * Create a builder with no default ID or key column.
         */
        public Builder() {
            defaultId = null;
            defaultKeys = null;
        }

        /**
         * Create a builder with a default ID and key columns.
         *
         * @param defaultId the default name of the ID columns
         * @param defaultKeys the default name of the key columns
         */
        public Builder(final String defaultId, final String... defaultKeys) {
            this.defaultId = defaultId;
            this.defaultKeys = defaultKeys;
        }

        /**
         * Add a table to the set of tables to be synchronized.
         *
         * @param name the key of the Table in our output map.
         * @param table the Table to add
         * @param idColumn the name of the ID column in the table, must be a long
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public Builder addTable(final String name, final Table table, final String idColumn,
                final String... keyColumns) {
            tables.add(new SyncTableDescription(name, table, idColumn, keyColumns));
            return this;
        }

        private void checkDefaultsInitialized() {
            if (defaultId == null) {
                throw new IllegalArgumentException(
                        "Can not specify table without an ID column unless the default ID has been set on the builder!");
            }
            if (defaultKeys == null) {
                throw new IllegalArgumentException(
                        "Can not specify table without a key column unless the default keys have been set on the builder!");
            }
        }

        /**
         * Add a table to the set of tables to be synchronized, using this builder's default ID and key column names.
         *
         * @param name the key of the Table in our output map.
         * @param table the Table to add
         *
         * @return this builder
         */
        public Builder addTable(final String name, final Table table) {
            checkDefaultsInitialized();
            return addTable(name, table, defaultId, defaultKeys);
        }

        /**
         * Set the default ID column name.
         *
         * @param id the new default ID column name
         *
         * @return this builder
         */
        public Builder defaultId(final String id) {
            this.defaultId = id;
            return this;
        }

        /**
         * Set the default key column name.
         *
         * @param keys the new default key column names
         *
         * @return this builder
         */
        public Builder defaultKeys(final String... keys) {
            defaultKeys = keys;
            return this;
        }

        /**
         * Instantiate the map of synchronized tables.
         *
         * This must be called under the UpdateGraphProcessor lock.
         *
         * @return a map with one entry for each input table
         */
        public Map<String, Table> build() {
            if (!tables.isEmpty()) {
                return new SyncTableFilter(tables).getMap();
            } else {
                throw new IllegalArgumentException(
                        "You must specify tables as parameters to the SyncTableFilter.Builder");
            }
        }
    }
}

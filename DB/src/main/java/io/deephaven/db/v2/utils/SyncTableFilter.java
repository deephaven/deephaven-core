/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.Pair;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
 * only rows with matching values in all of the input tables. For example, if you have input tables "a" "b" and "c",
 * with a key of "USym", if "a" has rows for SPY with IDs of 1, 2, and 3, but "b" and "c" only have rows for "2", the
 * filter will pass through the rows with ID 2. When both "b" and "c" have SPY rows for 3, then the rows for ID 2 are
 * removed and the rows for ID 3 are added to the result tables.
 *
 * The SyncTableFilter is configured through the Builder inner class. Tables are added to the builder, providing a name
 * for each table. The return value is a TableMap of the results, each filtered input table is accessible according to
 * the name provided to the builder.
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
    private final Index[] resultIndex;

    private final TupleSource[] keySources;
    private final List<ColumnSource<Long>> idSources;
    private final List<Map<Object, KeyState>> objectToState;
    private final TObjectLongMap<Object> minimumid;
    private final HashSet<Object> pendingKeys = new HashSet<>();
    private final List<ListenerRecorder> recorders;

    private static class KeyState {
        Index pendingRows = Index.FACTORY.getEmptyIndex();
        Index matchedRows = Index.FACTORY.getEmptyIndex();
        Index.SequentialBuilder unprocessedBuilder = null;
        Index.SequentialBuilder currentIdBuilder = null;
        final TLongArrayList unprocessedIds = new TLongArrayList();
        int sortStart = -1;
    }

    private static abstract class SyncDescription {
        final String name;
        final String idColumn;
        final String[] keyColumns;

        SyncDescription(final String name, final String idColumn, final String[] keyColumns) {
            this.name = name;
            this.idColumn = idColumn;
            this.keyColumns = keyColumns;
        }
    }

    private static class SyncTableDescription extends SyncDescription {
        final Table table;

        SyncTableDescription(final String name, final Table table, final String idColumn, final String[] keyColumns) {
            super(name, idColumn, keyColumns);
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

    private static class SyncTableMapDescription extends SyncDescription {
        final TableMap tableMap;

        SyncTableMapDescription(final String name, final TableMap tableMap, final String idColumn,
                final String[] keyColumns) {
            super(name, idColumn, keyColumns);
            this.tableMap = tableMap;
        }

        SyncTableDescription forPartition(Object partitionKey) {
            return new SyncTableDescription(name, tableMap.get(partitionKey), idColumn, keyColumns);
        }
    }

    private SyncTableFilter(final List<SyncTableDescription> tables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("No tables specified!");
        }

        if (tables.stream().anyMatch(t -> t.table.isLive())) {
            LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        }

        // through the builder only
        this.tables = tables;
        final int tableCount = tables.size();
        this.objectToState = new ArrayList<>(tableCount);
        this.keySources = new TupleSource[tableCount];
        this.idSources = new ArrayList<>(tableCount);
        this.results = new QueryTable[tableCount];
        this.resultIndex = new Index[tableCount];
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
            resultIndex[ii] = Index.FACTORY.getEmptyIndex();
            results[ii] = (QueryTable) ((QueryTable) std.table).getSubTable(resultIndex[ii], null, mergedListener);

            final ListenerRecorder listenerRecorder =
                    new ListenerRecorder("SyncTableFilter(" + std.name + ")", (DynamicTable) std.table, results[ii]);
            ((DynamicTable) std.table).listenForUpdates(listenerRecorder);
            recorders.add(listenerRecorder);

            consumeRows(ii, std.table.getIndex());
        }

        recorders.forEach(lr -> lr.setMergedListener(mergedListener));

        final Pair<HashSet<Object>, HashSet<Object>> hashSetPair = processPendingKeys();
        final HashSet<Object> keysToRefilter = hashSetPair.first;
        Assert.eqZero(hashSetPair.second.size(), "hashSetPair.second.size()");
        for (int tt = 0; tt < tableCount; tt++) {
            final Index.RandomBuilder addedBuilder = Index.FACTORY.getRandomBuilder();
            for (Object key : keysToRefilter) {
                final KeyState state = objectToState.get(tt).get(key);
                doMatch(tt, state, minimumid.get(key));
                addedBuilder.addIndex(state.matchedRows);
            }
            resultIndex[tt].insert(addedBuilder.getIndex());
        }
        keysToRefilter.clear();
    }

    class MergedSyncListener extends io.deephaven.db.v2.MergedListener {
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
                    if (recorder.getRemoved().nonempty()) {
                        throw new IllegalStateException("Can not process removed rows in SyncTableFilter!");
                    }
                    if (recorder.getShifted().nonempty()) {
                        throw new IllegalStateException("Can not process shifted rows in SyncTableFilter!");
                    }
                    final Index addedAndModified = recorder.getAdded().union(recorder.getModified());
                    consumeRows(rr, addedAndModified);
                }
            }

            final Pair<HashSet<Object>, HashSet<Object>> hashSetPair = processPendingKeys();
            final HashSet<Object> keysToRefilter = hashSetPair.first;
            final HashSet<Object> keysWithNewCurrentRows = hashSetPair.second;
            for (int tt = 0; tt < objectToState.size(); tt++) {
                final Index.RandomBuilder removedBuilder = Index.FACTORY.getRandomBuilder();
                final Index.RandomBuilder addedBuilder = Index.FACTORY.getRandomBuilder();
                for (Object key : keysToRefilter) {
                    final KeyState state = objectToState.get(tt).get(key);
                    removedBuilder.addIndex(state.matchedRows);
                    doMatch(tt, state, minimumid.get(key));
                    addedBuilder.addIndex(state.matchedRows);
                }

                for (Object key : keysWithNewCurrentRows) {
                    final KeyState state = objectToState.get(tt).get(key);
                    if (state.currentIdBuilder == null) {
                        continue;
                    }
                    if (!keysToRefilter.contains(key)) {
                        // if we did not refilter this key; then we should add the currently matched values,
                        // otherwise we ignore them because they have already been superseded
                        final Index newlyMatchedRows = state.currentIdBuilder.getIndex();
                        state.matchedRows.insert(newlyMatchedRows);
                        newlyMatchedRows.remove(resultIndex[tt]);
                        addedBuilder.addIndex(newlyMatchedRows);
                    }
                    state.currentIdBuilder = null;
                }

                final Index removed = removedBuilder.getIndex();
                final Index added = addedBuilder.getIndex();
                resultIndex[tt].remove(removed);
                resultIndex[tt].insert(added);

                final Index addedAndRemoved = added.intersect(removed);

                final Index modified;
                if (recorders.get(tt).getNotificationStep() == currentStep) {
                    modified = recorders.get(tt).getModified().intersect(resultIndex[tt]);
                    modified.remove(added);
                    modified.insert(addedAndRemoved);
                } else {
                    modified = addedAndRemoved;
                }

                if (added.nonempty() || removed.nonempty() || modified.nonempty()) {
                    results[tt].notifyListeners(added, removed, modified);
                }
            }
            keysToRefilter.clear();
        }

        @Override
        protected void notifyOnError(Exception updateException) {
            for (QueryTable queryTable : results) {
                notifyOnError(updateException, queryTable);
            }
        }

        @Override
        protected boolean systemicResult() {
            return Arrays.stream(results).anyMatch(SystemicObjectTracker::isSystemic);
        }
    }

    private void doMatch(final int tableIndex, final KeyState state, final long matchValue) {
        final Index.SequentialBuilder matchedBuilder = Index.FACTORY.getSequentialBuilder();
        final Index.SequentialBuilder pendingBuilder = Index.FACTORY.getSequentialBuilder();
        final WritableLongChunk<Attributes.OrderedKeyIndices> keyIndices =
                WritableLongChunk.makeWritableChunk(CHUNK_SIZE);

        try (final OrderedKeys.Iterator okit = state.pendingRows.getOrderedKeysIterator();
                final ColumnSource.GetContext getContext = idSources.get(tableIndex).makeGetContext(CHUNK_SIZE)) {
            while (okit.hasMore()) {
                final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(CHUNK_SIZE);
                chunkOk.fillKeyIndicesChunk(keyIndices);
                final LongChunk<? extends Attributes.Values> idChunk =
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

        state.pendingRows = pendingBuilder.getIndex();
        state.matchedRows = matchedBuilder.getIndex();
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
                    keyState.pendingRows.insert(keyState.unprocessedBuilder.getIndex());
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

    private void consumeRows(final int tableIndex, final Index index) {
        // in Treasure the TupleSource will handle chunks better
        final WritableObjectChunk valuesChunk = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
        final WritableLongChunk<Attributes.Values> idChunk = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        final WritableLongChunk<Attributes.OrderedKeyIndices> keyIndicesChunk =
                WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        final ColumnSource<Long> idSource = idSources.get(tableIndex);
        try (final OrderedKeys.Iterator okIt = index.getOrderedKeysIterator();
                final ColumnSource.FillContext fillContext = idSource.makeFillContext(CHUNK_SIZE)) {
            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);
                chunkOk.fillKeyIndicesChunk(keyIndicesChunk);
                valuesChunk.setSize(0);
                chunkOk.forEachLong(idx -> {
                    Object tuple = keySources[tableIndex].createTuple(idx);
                    // noinspection unchecked
                    valuesChunk.add(tuple);
                    return true;
                });
                idSource.fillChunk(fillContext, idChunk, chunkOk);

                // TODO: when we are in Treasure, we should sort this so we do not need to repeatedly look things up
                // TODO: In Treasure we can also use current factories for our index
                for (int ii = 0; ii < idChunk.size(); ++ii) {
                    final Object key = valuesChunk.get(ii);
                    pendingKeys.add(key);
                    final KeyState currentState =
                            objectToState.get(tableIndex).computeIfAbsent(key, (k) -> new KeyState());
                    if (currentState.unprocessedBuilder == null) {
                        currentState.unprocessedBuilder = Index.FACTORY.getSequentialBuilder();
                    }
                    final long minid = minimumid.get(key);

                    final long id = idChunk.get(ii);
                    if (id == QueryConstants.NULL_LONG || (id < minid)) {
                        continue;
                    }
                    if (id == minid) {
                        if (currentState.currentIdBuilder == null) {
                            currentState.currentIdBuilder = Index.FACTORY.getSequentialBuilder();
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

    private TableMap getTableMap() {
        final LocalTableMap map = new LocalTableMap(null);
        for (int ii = 0; ii < tables.size(); ii++) {
            map.put(tables.get(ii).name, results[ii]);
        }
        return map;
    }

    private static class InsertKeySetNotification extends AbstractNotification implements NotificationQueue.Dependency {
        private final Map<Object, Set<String>> pendingPartitions;
        private final LocalTableMap result;
        @NotNull
        private final List<SyncTableMapDescription> descriptions;
        private final List<NotificationQueue.Dependency> dependencies;
        private final Set<String> allKeys;

        long notificationClock = -1;
        long notificationCompletedClock = -1;
        long queuedNotificationClock = -1;

        private InsertKeySetNotification(Map<Object, Set<String>> pendingPartitions, LocalTableMap result,
                List<SyncTableMapDescription> descriptions) {
            super(false);

            this.pendingPartitions = pendingPartitions;
            this.result = result;
            this.descriptions = descriptions;

            dependencies = descriptions.stream().map(stmd -> ((NotificationQueue.Dependency) stmd.tableMap))
                    .collect(Collectors.toList());
            allKeys = descriptions.stream().map(desc -> desc.name).collect(Collectors.toSet());
        }

        @Override
        public boolean canExecute(final long step) {
            return dependencies.stream().allMatch((final NotificationQueue.Dependency dep) -> dep.satisfied(step));
        }

        @Override
        public void run() {
            synchronized (this) {
                notificationClock = LogicalClock.DEFAULT.currentStep();
            }
            createFullyPopulatedKeys();
            synchronized (this) {
                notificationCompletedClock = LogicalClock.DEFAULT.currentStep();
            }
        }

        private void notifyChanges() {
            final long currentStep = LogicalClock.DEFAULT.currentStep();

            synchronized (this) {
                if (notificationClock == currentStep) {
                    throw new IllegalStateException(
                            "MergedListener was fired before both all listener records completed: listener="
                                    + System.identityHashCode(this) + ", currentStep=" + currentStep);
                }

                // we've already got something in the notification queue that has not yet been executed for the current
                // step.
                if (queuedNotificationClock == currentStep) {
                    return;
                }

                // Otherwise we should have already flushed that notification.
                Assert.assertion(queuedNotificationClock == notificationClock,
                        "queuedNotificationClock == notificationClock", queuedNotificationClock,
                        "queuedNotificationClock", notificationClock, "notificationClock", currentStep, "currentStep",
                        this, "MergedListener");

                queuedNotificationClock = currentStep;
                LiveTableMonitor.DEFAULT.addNotification(this);
            }
        }

        private void createFullyPopulatedKeys() {
            for (Iterator<Map.Entry<Object, Set<String>>> it = pendingPartitions.entrySet().iterator(); it.hasNext();) {
                final Map.Entry<Object, Set<String>> partitionKeyAndPopulatedNames = it.next();
                if (!partitionKeyAndPopulatedNames.getValue().equals(allKeys)) {
                    continue;
                }

                final Object partitionKey = partitionKeyAndPopulatedNames.getKey();

                final List<SyncTableDescription> syncTableDescriptions =
                        descriptions.stream().map(stmd -> stmd.forPartition(partitionKey)).collect(Collectors.toList());
                final TableMap syncFiltered = new SyncTableFilter(syncTableDescriptions).getTableMap();
                result.addParentReference(syncFiltered);
                for (Object tableName : syncFiltered.getKeySet()) {
                    final SmartKey transformedKey;
                    if (partitionKey instanceof SmartKey) {
                        final Object[] partitionKeyArray = ((SmartKey) partitionKey).values_;
                        final Object[] newKey = Arrays.copyOf(partitionKeyArray, partitionKeyArray.length + 1);
                        newKey[newKey.length - 1] = tableName;
                        transformedKey = new SmartKey(newKey);
                    } else {
                        transformedKey = new SmartKey(partitionKey, tableName);
                    }
                    result.put(transformedKey, syncFiltered.get(tableName));
                }
                it.remove();
            }
        }

        @Override
        public boolean satisfied(final long step) {
            if (notificationCompletedClock == step) {
                // we've already fired and done our work
                return true;
            }
            if (queuedNotificationClock == step) {
                // we haven't done our work, but we must do work this cycle
                return false;
            }
            // otherwise, we are satisfied if we could not possibly be notified
            return canExecute(step);
        }

        @Override
        public LogOutput append(LogOutput output) {
            return output.append("SyncTableFilter.InsertKeyNotification{").append(System.identityHashCode(this))
                    .append("}");
        }
    }

    private static TableMap createTableMapAdapter(List<SyncTableMapDescription> descriptions) {
        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();

        final int mapCount = descriptions.size();

        final LocalTableMap result = new LocalTableMap(null);


        final Map<Object, Set<String>> pendingPartitions = new ConcurrentHashMap<>();


        final InsertKeySetNotification notification =
                new InsertKeySetNotification(pendingPartitions, result, descriptions);

        final TableMap.KeyListener[] keyListeners = new TableMap.KeyListener[mapCount];
        for (int ii = 0; ii < descriptions.size(); ++ii) {
            final SyncTableMapDescription syncTableMapDescription = descriptions.get(ii);
            final String name = syncTableMapDescription.name;
            keyListeners[ii] = key -> {
                markTablePopulated(pendingPartitions, name, key);
                notification.notifyChanges();
            };
            syncTableMapDescription.tableMap.addKeyListener(keyListeners[ii]);

            for (Object partitionKey : syncTableMapDescription.tableMap.getKeySet()) {
                markTablePopulated(pendingPartitions, name, partitionKey);
            }
        }

        notification.createFullyPopulatedKeys();

        result.addParentReference(keyListeners);
        result.setDependency(notification);

        return result;
    }

    private static void markTablePopulated(Map<Object, Set<String>> pendingPartitions, String name, Object key) {
        final Set<String> presentTables =
                pendingPartitions.computeIfAbsent(key, (k) -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        presentTables.add(name);
    }

    /**
     * Produce a TableMap of synchronized tables.
     *
     * <p>
     * You may include either Tables or TableMaps, but not both. When Tables are included, the result of the build call
     * is a TableMap with a String key that corresponds to the name of the input table. When TableMaps are added, the
     * result is a TableMap with composite keys (SmartKeys) that are prefixed with the keys from the input TableMap,
     * with a last element that is the name of the source passed to the builder.
     * </p>
     */
    public static class Builder {
        private String defaultId;
        private String[] defaultKeys;
        private final List<SyncTableDescription> tables = new ArrayList<>();
        private final List<SyncTableMapDescription> tableMaps = new ArrayList<>();

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
         * @param name the key of the Table in our output TableMap.
         * @param table the Table to add
         * @param idColumn the name of the ID column in the table, must be a long
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         * @return this builder
         */
        public Builder addTable(final String name, final Table table, final String idColumn,
                final String... keyColumns) {
            if (!tableMaps.isEmpty()) {
                throw new IllegalArgumentException("Can not mix Tables and TableMaps in a SyncTableFilter");
            }
            tables.add(new SyncTableDescription(name, table, idColumn, keyColumns));
            return this;
        }

        private void checkDefaultsInitialized(final String type) {
            if (defaultId == null) {
                throw new IllegalArgumentException("Can not specify " + type
                        + " without an ID column unless the default ID has been set on the builder!");
            }
            if (defaultKeys == null) {
                throw new IllegalArgumentException("Can not specify " + type
                        + " without a key columns unless the default keys have been set on the builder!");
            }
        }

        /**
         * Add a table to the set of tables to be synchronized, using this builder's default ID and key column names.
         *
         * @param name the key of the Table in our output TableMap.
         * @param table the Table to add
         *
         * @return this builder
         */
        public Builder addTable(final String name, final Table table) {
            checkDefaultsInitialized("table");
            return addTable(name, table, defaultId, defaultKeys);
        }

        /**
         * Add a TableMap to the set of TableMaps to be synchronized.
         *
         * @param name the key of the Table in our output TableMap.
         * @param tableMap the TableMap to add
         *
         * @return this builder
         */
        public Builder addTableMap(String name, TableMap tableMap) {
            checkDefaultsInitialized("TableMap");
            return addTableMap(name, tableMap, defaultId, defaultKeys);
        }

        /**
         * Add a TableMap to the set of TableMaps to be synchronized.
         *
         * @param name the key of the Table in our output TableMap.
         * @param tableMap the TableMap to add
         * @param idColumn the name of the ID column in the table, must be a long
         * @param keyColumns the key columns, each key is coordinated independently of the other keys
         *
         * @return this builder
         */
        public Builder addTableMap(String name, TableMap tableMap, final String idColumn, final String... keyColumns) {
            if (!tables.isEmpty()) {
                throw new IllegalArgumentException("Can not mix Tables and TableMaps in a SyncTableFilter");
            }
            tableMaps.add(new SyncTableMapDescription(name, tableMap, idColumn, keyColumns));
            return this;
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
         * Instantiate the TableMap of synchronized tables.
         *
         * This must be called under the LiveTableMonitor lock.
         *
         * @return a TableMap with one entry for each input table
         */
        public TableMap build() {
            if (!tables.isEmpty()) {
                return new SyncTableFilter(tables).getTableMap();
            } else if (!tableMaps.isEmpty()) {
                return createTableMapAdapter(tableMaps);
            } else {
                throw new IllegalArgumentException(
                        "You must specify tables or TableMaps as parameters to the SyncTableFilter.Builder");
            }
        }
    }
}

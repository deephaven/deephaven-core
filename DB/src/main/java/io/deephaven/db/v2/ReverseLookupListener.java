package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.annotations.TestUseOnly;
import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.set.hash.THashSet;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Maintains a map from key column values to their index.
 *
 * This allows you to quickly find a row based on a unique key on a ticking table, without the need for searching the
 * entire table.
 *
 * Note: The key column values must be unique.
 */
public class ReverseLookupListener extends LivenessArtifact implements ReverseLookup, DynamicNode, NotificationStepSource {
    private static final long NO_ENTRY_VALUE = -2;
    private static final long REMOVED_ENTRY_VALUE = -3;

    private final String[] keyColumnNames;
    private final ColumnSource[] columns;
    private final TObjectLongHashMap<Object> map;
    private final boolean ignoreNull;
    private final InternalListener listener;

    private class InternalListener extends InstrumentedListenerAdapter implements NotificationStepSource, NotificationStepReceiver  {
        private final TObjectLongHashMap<Object> prevMap;
        private final Set<Object> modifiedThisCycle = new THashSet<>();
        private volatile long lastNotificationStep = NULL_NOTIFICATION_STEP;

        InternalListener(String description, DynamicTable source, boolean retain) {
            super(description, source, retain);
            prevMap = new TObjectLongHashMap<>(source.isRefreshing() ? 2*source.intSize() : 0, 0.75f, NO_ENTRY_VALUE);
            modifiedThisCycle.clear();
        }

        @Override
        public void onUpdate(final Index added, final Index removed, final Index modified) {
            synchronized (ReverseLookupListener.this) {
                // Note that lastNotificationStep will change before we are technically satisfied, but it doesn't
                // matter; we aren't fully updated yet, but we rely on synchronization on the enclosing RLL to prevent
                // inconsistent data access. By changing the step as early as we know we can we allow concurrent
                // consumers to avoid using a WaitNotification and just rely on our locking.
                lastNotificationStep = LogicalClock.DEFAULT.currentStep();
                prevMap.clear();
                removeEntries(removed);
                modifyEntries(modified);
                addEntries(added, false, () -> {});
                modifiedThisCycle.clear();
            }
        }

        private void removeEntries(Index index) {
            for (final Index.Iterator it = index.iterator(); it.hasNext(); ) {
                final long row = it.nextLong();
                final Object keyToReverse = getPrevKey(row);
                if(ignoreNull && keyToReverse == null) {
                    continue;
                }

                final long oldRow = map.remove(keyToReverse);
                if (oldRow == map.getNoEntryValue()) {
                    throw Assert.statementNeverExecuted("Removed value not in reverse lookup map: row=" + row + ", key=" + keyToReverse);
                }
                setPrevious(keyToReverse, oldRow);
            }
        }

        private void modifyEntries(Index index) {
            for (final Index.Iterator it = index.iterator(); it.hasNext(); ) {
                final long row = it.nextLong();
                final Object keyToReverse = getPrevKey(row);
                final Object newKey = getKey(row);
                if (Objects.equals(keyToReverse, newKey)) {
                    continue;
                }

                final long oldRow;

                // We only want to remove keys from the mapping that haven't already been modified.
                if((!ignoreNull || keyToReverse != null) && !modifiedThisCycle.contains(keyToReverse)) {
                    oldRow = map.remove(keyToReverse);
                    if (oldRow == map.getNoEntryValue()) {
                        throw Assert.statementNeverExecuted("Removed value not in reverse lookup map: row=" + row + ", key=" + keyToReverse);
                    }
                } else {
                    oldRow = NO_ENTRY_VALUE;
                }

                if(!ignoreNull || newKey != null) {
                    // Take into account that the newKey may already be mapped somewhere, and in that case
                    // should be added to the previous map so we don't lose that component.
                    setPrevious(newKey, map.put(newKey, row));
                }

                setPrevious(keyToReverse, oldRow);
            }
        }

        private void setPrevious(Object keyToReverse, long oldRow) {
            if (modifiedThisCycle.add(keyToReverse)) {
                if (oldRow == NO_ENTRY_VALUE) {
                    prevMap.put(keyToReverse, REMOVED_ENTRY_VALUE);
                } else {
                    prevMap.put(keyToReverse, oldRow);
                }
            }
        }

        @Override
        public long getLastNotificationStep() {
            return lastNotificationStep;
        }

        @Override
        public void setLastNotificationStep(long step) {
            lastNotificationStep = step;
        }

        long getPrev(Object key) {
            if (LogicalClock.DEFAULT.currentStep() == lastNotificationStep) {
                final long prevValue = prevMap.get(key);
                if (prevValue != NO_ENTRY_VALUE) {
                    return prevValue == REMOVED_ENTRY_VALUE ? NO_ENTRY_VALUE : prevValue;
                }
            }

            return map.get(key);
        }

        @Override
        public String toString() {
            return "{lastNotificationStep=" + lastNotificationStep +
                    ", modifiedThisCycle.size=" + modifiedThisCycle.size() +
                    ", prevMap.size=" + prevMap.size() + "}";
        }
    }


    // we need to hold onto our swap listener, because nothing else will hold onto it for us
    @ReferentialIntegrity
    private Object reference;

    public static ReverseLookupListener makeReverseLookupListenerWithSnapshot(BaseTable source, String... columns) {
        final SwapListener swapListener;
        if (source.isRefreshing()) {
            swapListener = new SwapListener(source);
            source.listenForUpdates(swapListener);
        } else {
            swapListener = null;
        }

        final Mutable<ReverseLookupListener> resultListener = new MutableObject<>();

        //noinspection AutoBoxing
        ConstructSnapshot.callDataSnapshotFunction(System.identityHashCode(source) + ": ",
                swapListener == null ? ConstructSnapshot.StaticSnapshotControl.INSTANCE : swapListener.makeSnapshotControl(),
                (usePrev, beforeClock) -> {
                    final ReverseLookupListener value = new ReverseLookupListener(source, false, usePrev, columns);
                    if (swapListener != null) {
                        swapListener.setListenerAndResult(value.listener, value.listener);
                        value.reference = swapListener;
                    }
                    resultListener.setValue(value);
                    return true;
                }
        );

        final ReverseLookupListener resultListenerValue = resultListener.getValue();
        if (swapListener != null) {
            resultListenerValue.listener.manage(swapListener);
        }
        return resultListenerValue;
    }

    public static ReverseLookupListener makeReverseLookupListenerWithLock(DynamicTable source, String... columns) {
        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        final ReverseLookupListener result = new ReverseLookupListener(source, columns);
        source.listenForUpdates(result.listener);
        return result;
    }

    /**
     * Prepare the parameter table for use with {@link Table#treeTable(String, String) tree table}
     *
     * @param preTree The tree to prepare
     * @param idColumn The column that will be used as the id for {@link Table#treeTable(String, String)}
     */
    @ScriptApi
    public static void prepareForTree(BaseTable preTree, String idColumn) {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (preTree) {
            if (preTree.hasAttribute(Table.PREPARED_RLL_ATTRIBUTE)) {
                return;
            }

            preTree.setAttribute(Table.PREPARED_RLL_ATTRIBUTE, makeReverseLookupListenerWithSnapshot(preTree, idColumn));
        }
    }

    private ReverseLookupListener(DynamicTable source, String... columns) {
        this(source, false, columns);
    }

    private ReverseLookupListener(DynamicTable source, boolean ignoreNull, String... columns) {
        this(source, ignoreNull, false, columns);
    }

    private ReverseLookupListener(DynamicTable source, boolean ignoreNull, boolean usePrev, String... columns) {
        this.keyColumnNames = columns;
        this.ignoreNull = ignoreNull;
        this.columns = Arrays.stream(columns).map(source::getColumnSource).toArray(ColumnSource[]::new);

        map = new TObjectLongHashMap<>(2*source.intSize(), 0.75f, NO_ENTRY_VALUE);
        try (final ReadOnlyIndex prevIndex = usePrev ? source.getIndex().getPrevIndex() : null) {
            addEntries(usePrev ? prevIndex : source.getIndex(), usePrev, () -> {
                if (source.isRefreshing()) {
                    ConstructSnapshot.failIfConcurrentAttemptInconsistent();
                }
            });
        }

        if(source.isRefreshing()) {
            this.listener = new InternalListener("ReverseLookup(" + Arrays.toString(columns) + ")", source, false);
            manage(listener);
        } else {
            this.listener = null;
        }
    }

    @Override
    public synchronized long get(Object key) {
        return map.get(key);
    }

    @Override
    public synchronized long getPrev(Object key) {
        return listener != null ? listener.getPrev(key) : map.get(key);
    }

    @Override
    public long getNoEntryValue() {
        return NO_ENTRY_VALUE;
    }

    /**
     * Returns an iterator to the underlying map of current values. This should only be used by unit tests, as the
     * iterator is not synchronized on the RLL and hence may become inconsistent.
     *
     * @return an iterator to the underlying map of values.
     */
    @TestUseOnly
    TObjectLongIterator<Object> iterator() {
        return map.iterator();
    }

    /**
     * Gets the key for a given row.
     * @param row the index value to retrieve the key for
     * @return an individual object or SmartKey for multi-column keys
     */
    protected Object getKey(long row) {
        return TableTools.getKey(columns, row);
    }

    /**
     * Gets the previous key for a given row.
     * @param row the index value to retrieve the previous key for
     * @return an individual object or SmartKey for multi-column keys
     */
    private Object getPrevKey(long row) {
        return TableTools.getPrevKey(columns, row);
    }

    private void addEntries(@NotNull final ReadOnlyIndex index, final boolean usePrev, @NotNull final Runnable consistencyChecker) {
        for (final ReadOnlyIndex.Iterator it = index.iterator(); it.hasNext(); ) {
            final long row = it.nextLong();
            final Object keyToReverse = usePrev ? getPrevKey(row) : getKey(row);
            if(ignoreNull && keyToReverse == null) {
                continue;
            }

            final long oldRow = map.put(keyToReverse, row);
            if (oldRow != map.getNoEntryValue()) {
                consistencyChecker.run();
                throw Assert.statementNeverExecuted("Duplicate value in reverse lookup map: row=" + row + ", oldRow=" + oldRow + ", key=" + keyToReverse);
            }

            if(listener != null) {
                listener.setPrevious(keyToReverse, oldRow);
            }
        }
    }

    @Override
    public String toString() {
        return "ReverseLookupListener{" +
                "map={size=" + (map == null ? 0 : map.size()) + "}" +
                "listener=" + listener +
                '}';
    }

    @Override
    public String[] getKeyColumns() {
        return keyColumnNames;
    }

    @Override
    public long getLastNotificationStep() {
        assertLive();
        return listener.getLastNotificationStep();
    }

    @Override
    public boolean satisfied(final long step) {
        assertLive();
        return listener.satisfied(step);
    }

    private void assertLive() {
        Assert.assertion(listener != null, "The base table was not live,  this method should not be invoked.");
    }

    @Override
    public boolean isRefreshing() {
        return listener != null;
    }

    @Override
    public boolean setRefreshing(boolean refreshing) {
        throw new UnsupportedOperationException("An RLL refreshing state is tied to the table it is mapping and can not be changed.");
    }

    @Override
    public void addParentReference(Object parent) {
        throw new UnsupportedOperationException("RLLs may not have parent references.");
    }
}

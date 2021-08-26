package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.WaitNotification;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexBuilder;
import io.deephaven.util.annotations.ReferentialIntegrity;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.LongFunction;

import static io.deephaven.db.tables.Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE;
import static io.deephaven.db.tables.Table.PREPARED_RLL_ATTRIBUTE;

/**
 * Apply filters, preserving parents.
 *
 * The TreeTableFilter takes a TreeTable and SelectFilters as input. The original source table is
 * filtered and any matching rows are included; as well as their ancestors. The result table is then
 * converted into a tree table using the original parameters.
 */
public class TreeTableFilter
    implements Function.Unary<Table, Table>, MemoizedOperationKey.Provider {
    private static final boolean DEBUG = io.deephaven.configuration.Configuration.getInstance()
        .getBooleanWithDefault("TreeTableFilter.debug", false);

    private static final Logger log = LoggerFactory.getLogger(TreeTableFilter.class);

    private final SelectFilter[] filters;
    private final TableDefinition origTableDefinition;

    private TreeTableFilter(Table source, SelectFilter[] filters) {
        this.filters = filters;
        this.origTableDefinition = source.getDefinition();

        for (final SelectFilter filter : filters) {
            filter.init(origTableDefinition);
        }
    }

    @Override
    public Table call(Table table) {
        return new State(table, origTableDefinition).getRaw();
    }

    @Override
    public MemoizedOperationKey getMemoKey() {
        if (Arrays.stream(filters).allMatch(SelectFilter::canMemoize)) {
            return new TreeTableFilterKey(filters);
        }

        return null;
    }

    private class State {
        private QueryTable filteredRaw;
        private final BaseTable source;

        /**
         * The complete index of our result table.
         */
        private Index resultIndex;
        /**
         * The index of values that match our desired filter.
         */
        private Index valuesIndex;
        /**
         * The index of ancestors of matching values.
         */
        private Index parentIndex;
        /**
         * For each parent key, a set of rows which directly descend from the parent.
         */
        private Map<Object, TLongSet> parentReferences;

        private final SelectFilter[] filters;
        private final ColumnSource parentSource;
        private final ColumnSource idSource;
        private final ReverseLookupListener reverseLookupListener;

        @ReferentialIntegrity
        final SwapListenerWithRLL swapListener;

        @ReferentialIntegrity
        TreeTableFilterListener treeListener;

        private final TreeTableInfo treeTableInfo;

        private State(Table table, TableDefinition origTableDefinition) {
            if (!(table instanceof HierarchicalTable)) {
                throw new IllegalArgumentException("Table is not a treeTable");
            }
            final HierarchicalTable hierarchicalTable = (HierarchicalTable) table;

            source = (BaseTable) hierarchicalTable.getSourceTable();
            final HierarchicalTableInfo sourceInfo = hierarchicalTable.getInfo();
            if (!(sourceInfo instanceof TreeTableInfo)) {
                throw new IllegalArgumentException("Table is not a treeTable");
            }
            treeTableInfo = (TreeTableInfo) sourceInfo;
            reverseLookupListener = Objects.requireNonNull(
                (ReverseLookupListener) table.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE));

            filters = TreeTableFilter.this.filters;

            // The filters have already been inited by here.
            Assert.eq(table.getDefinition(), "Applied table.definition", origTableDefinition,
                "Original definition");

            parentSource = source.getColumnSource(treeTableInfo.parentColumn);
            idSource = source.getColumnSource(treeTableInfo.idColumn);

            if (source.isRefreshing()) {
                swapListener = new SwapListenerWithRLL(source, reverseLookupListener);
                source.listenForUpdates(swapListener);
                ConstructSnapshot.callDataSnapshotFunction(System.identityHashCode(source) + ": ",
                    swapListener.makeSnapshotControl(),
                    (usePrev, beforeClockValue) -> {
                        doInitialFilter(usePrev);
                        return true;
                    });
            } else {
                swapListener = null;
                doInitialFilter(false);
            }
        }

        private void doInitialFilter(final boolean usePrev) {
            valuesIndex = doValueFilter(usePrev, source.getIndex());

            parentReferences = new HashMap<>(valuesIndex.intSize("parentReferenceMap"));
            parentIndex = computeParents(usePrev, valuesIndex);
            resultIndex = valuesIndex.union(parentIndex);
            resultIndex.initializePreviousValue();

            validateState(usePrev);

            filteredRaw = (QueryTable) source.getSubTable(resultIndex);
            if (swapListener != null) {
                treeListener = new TreeTableFilterListener("treeTable filter", source, filteredRaw);
                swapListener.setListenerAndResult(treeListener, filteredRaw);
                filteredRaw.addParentReference(swapListener);
                filteredRaw.addParentReference(treeListener);
            }

            // We can re-use the RLL when filtering as long as we are sure to check for existence in
            // the
            // sub table indices. Sticking this annotation here will let QueryTable know it can
            // re-use it.
            filteredRaw.setAttribute(PREPARED_RLL_ATTRIBUTE, reverseLookupListener);
        }

        private Table getRaw() {
            return filteredRaw;
        }

        private void validateState(final boolean usePrev) {
            if (!DEBUG) {
                return;
            }

            final Index union = valuesIndex.union(parentIndex);

            if (!union.equals(resultIndex)) {
                throw new IllegalStateException();
            }

            final Index expectedIndex = doValueFilter(usePrev, source.getIndex());

            final Map<Object, TLongSet> expectedParents = new HashMap<>();

            if (!expectedIndex.subsetOf(source.getIndex())) {
                throw new IllegalStateException("Bad refilter!");
            }

            if (!expectedIndex.equals(valuesIndex)) {
                final Index missing = expectedIndex.minus(valuesIndex);
                final Index extraValues = valuesIndex.minus(expectedIndex);
                throw new IllegalStateException(
                    "Inconsistent included Values: missing=" + missing + ", extra=" + extraValues
                        + ", expected=" + expectedIndex + ", valuesIndex=" + valuesIndex);
            }

            TLongArrayList parentsToProcess = new TLongArrayList();
            expectedIndex.forEach(parentsToProcess::add);

            final Index sourceIndex =
                usePrev ? source.getIndex().getPrevIndex() : source.getIndex();
            do {
                final TLongArrayList newParentKeys = new TLongArrayList();
                for (final TLongIterator it = parentsToProcess.iterator(); it.hasNext();) {
                    final long row = it.next();
                    final Object parent =
                        usePrev ? parentSource.getPrev(row) : parentSource.get(row);
                    if (parent == null) {
                        continue;
                    }
                    expectedParents.computeIfAbsent(parent, x -> new TLongHashSet()).add(row);
                    final long parentRow = usePrev ? reverseLookupListener.getPrev(parent)
                        : reverseLookupListener.get(parent);
                    if (parentRow == reverseLookupListener.getNoEntryValue()) {
                        continue;
                    }
                    if (sourceIndex.find(parentRow) < 0) {
                        throw new IllegalStateException(
                            "Reverse Lookup Listener points at row " + parentRow + " for " + parent
                                + ", but the row is not in the index=" + source.getIndex());
                    }
                    newParentKeys.add(parentRow);
                }
                parentsToProcess = newParentKeys;
            } while (!parentsToProcess.isEmpty());

            final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();

            parentReferences.forEach((parentValue, set) -> {
                final TLongSet actualSet = parentReferences.get(parentValue);
                final TLongSet expectedSet = expectedParents.get(parentValue);
                if (!actualSet.equals(expectedSet)) {
                    throw new IllegalStateException("Parent set mismatch " + parentValue
                        + ", expected=" + expectedSet + ", actual=" + actualSet);
                }

                final long parentKey = usePrev ? reverseLookupListener.getPrev(parentValue)
                    : reverseLookupListener.get(parentValue);
                if (parentKey != reverseLookupListener.getNoEntryValue()) {
                    // then we should have it in our index
                    builder.addKey(parentKey);
                    final long position = parentIndex.find(parentKey);
                    if (position < 0) {
                        throw new IllegalStateException("Could not find parent in our result: "
                            + parentValue + ", key=" + parentKey);
                    }
                }
            });

            final Index expectedParentIndex = builder.getIndex();
            if (!expectedParentIndex.equals(parentIndex)) {
                throw new IllegalStateException();
            }
        }

        private void removeValues(Index rowsToRemove) {
            valuesIndex.remove(rowsToRemove);
            removeParents(rowsToRemove);
        }

        private void removeParents(Index rowsToRemove) {
            final Map<Object, TLongSet> parents =
                generateParentReferenceMap(rowsToRemove, parentSource::getPrev);

            final IndexBuilder builder = Index.FACTORY.getRandomBuilder();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator =
                    parents.entrySet().iterator();
                final Map.Entry<Object, TLongSet> entry = iterator.next();
                final Object parent = entry.getKey();
                final TLongSet references = entry.getValue();
                iterator.remove();

                final long parentKey = reverseLookupListener.getPrev(parent);

                final TLongSet parentSet = parentReferences.get(parent);
                if (parentSet != null) {
                    parentSet.removeAll(references);

                    if (parentSet.isEmpty()) {
                        parentReferences.remove(parent);
                        if (parentKey != reverseLookupListener.getNoEntryValue()) {
                            builder.addKey(parentKey);

                            if (valuesIndex.find(parentKey) < 0) {
                                final Object grandParentId = parentSource.getPrev(parentKey);
                                if (grandParentId != null) {
                                    parents.computeIfAbsent(grandParentId, x -> new TLongHashSet())
                                        .add(parentKey);
                                }
                            }
                        }
                    }
                }
            }

            parentIndex.remove(builder.getIndex());
        }

        private Index doValueFilter(boolean usePrev, Index rowsToFilter) {
            for (final SelectFilter filter : filters) {
                rowsToFilter = filter.filter(rowsToFilter, source.getIndex(), source, usePrev);
            }

            return rowsToFilter;
        }

        private Index checkForResurrectedParent(Index rowsToCheck) {
            final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();

            for (final Index.Iterator it = rowsToCheck.iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final Object id = idSource.get(key);

                if (parentReferences.containsKey(id)) {
                    builder.appendKey(key);
                }
            }

            return builder.getIndex();
        }

        private Index computeParents(final boolean usePrev, @NotNull final Index rowsToParent) {
            final Map<Object, TLongSet> parents = generateParentReferenceMap(rowsToParent,
                usePrev ? parentSource::getPrev : parentSource::get);

            final IndexBuilder builder = Index.FACTORY.getRandomBuilder();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator =
                    parents.entrySet().iterator();
                final Map.Entry<Object, TLongSet> entry = iterator.next();
                final Object parent = entry.getKey();
                final TLongSet references = entry.getValue();
                iterator.remove();

                final long parentKey = usePrev ? reverseLookupListener.getPrev(parent)
                    : reverseLookupListener.get(parent);
                if (parentKey != reverseLookupListener.getNoEntryValue()) {
                    builder.addKey(parentKey);
                    final Object grandParentId =
                        usePrev ? parentSource.getPrev(parentKey) : parentSource.get(parentKey);
                    if (grandParentId != null) {
                        parents.computeIfAbsent(grandParentId, x -> new TLongHashSet())
                            .add(parentKey);
                    }
                }

                parentReferences.computeIfAbsent(parent, x -> new TLongHashSet())
                    .addAll(references);
            }

            return builder.getIndex();
        }

        @NotNull
        private Map<Object, TLongSet> generateParentReferenceMap(Index rowsToParent,
            LongFunction<Object> getValue) {
            final Map<Object, TLongSet> parents = new LinkedHashMap<>(rowsToParent.intSize());
            for (final Index.Iterator it = rowsToParent.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object parentId = getValue.apply(row);
                if (parentId != null) {
                    parents.computeIfAbsent(parentId, (x) -> new TLongHashSet()).add(row);
                }
            }
            return parents;
        }

        private class TreeTableFilterListener extends BaseTable.ShiftAwareListenerImpl {
            final ModifiedColumnSet inputColumns;

            TreeTableFilterListener(String description, DynamicTable parent, QueryTable dependent) {
                super(description, parent, dependent);
                inputColumns =
                    source.newModifiedColumnSet(treeTableInfo.idColumn, treeTableInfo.parentColumn);
                Arrays.stream(filters).forEach(filter -> {
                    for (final String column : filter.getColumns()) {
                        inputColumns.setAll(column);
                    }
                });
            }

            @Override
            public void onUpdate(final Update upstream) {
                // TODO: make chunk happy

                final Update downstream = new Update();

                final long rllLastStep = reverseLookupListener.getLastNotificationStep();
                final long sourceLastStep = source.getLastNotificationStep();

                if (rllLastStep != sourceLastStep) {
                    throw new IllegalStateException("RLL was updated in a different cycle! Rll: "
                        + rllLastStep + " source: " + sourceLastStep);
                }

                // We can ignore modified while updating if columns we care about were not touched.
                final boolean useModified = upstream.modifiedColumnSet.containsAny(inputColumns);

                // Must take care of removed here, because these rows are not valid in post shift
                // space.
                downstream.removed = resultIndex.extract(upstream.removed);

                try (
                    final Index allRemoved =
                        useModified ? upstream.removed.union(upstream.getModifiedPreShift()) : null;
                    final Index valuesToRemove =
                        (useModified ? allRemoved : upstream.removed).intersect(valuesIndex);
                    final Index removedParents =
                        (useModified ? allRemoved : upstream.removed).intersect(parentIndex)) {

                    removeValues(valuesToRemove);
                    parentIndex.remove(removedParents);
                    removeParents(removedParents);
                }

                // Now we must shift all maintained state.
                upstream.shifted.forAllInIndex(resultIndex, (key, delta) -> {
                    final Object parentId = parentSource.getPrev(key);
                    if (parentId == null) {
                        return;
                    }
                    final TLongSet parentSet = parentReferences.get(parentId);
                    if (parentSet == null) {
                        return;
                    }
                    if (parentSet.remove(key)) {
                        parentSet.add(key + delta);
                    }
                });
                upstream.shifted.apply(valuesIndex);
                upstream.shifted.apply(parentIndex);
                upstream.shifted.apply(resultIndex);

                // Finally handle added sets.
                try (final Index addedAndModified = upstream.added.union(upstream.modified);
                    final Index newFiltered = doValueFilter(false, addedAndModified);
                    final Index resurrectedParents = checkForResurrectedParent(addedAndModified);
                    final Index newParents = computeParents(false, newFiltered);
                    final Index newResurrectedParents = computeParents(false, resurrectedParents)) {


                    valuesIndex.insert(newFiltered);
                    parentIndex.insert(newParents);
                    parentIndex.insert(resurrectedParents);
                    parentIndex.insert(newResurrectedParents);
                }

                // Compute expected results and the sets we will propagate to child listeners.
                try (final Index result = valuesIndex.union(parentIndex);
                    final Index resultRemovals = resultIndex.minus(result)) {
                    downstream.added = result.minus(resultIndex);
                    resultIndex.update(downstream.added, resultRemovals);

                    downstream.modified = upstream.modified.intersect(resultIndex);
                    downstream.modified.remove(downstream.added);

                    // convert post filter removals into pre-shift space -- note these rows must
                    // have previously existed
                    upstream.shifted.unapply(resultRemovals);
                    downstream.removed.insert(resultRemovals);
                }

                downstream.shifted = upstream.shifted;
                downstream.modifiedColumnSet = upstream.modifiedColumnSet; // note that dependent is
                                                                           // a subTable

                filteredRaw.notifyListeners(downstream);

                validateState(false);
            }

            @Override
            public boolean canExecute(final long step) {
                return super.canExecute(step) && reverseLookupListener.satisfied(step);
            }
        }
    }

    private static class TreeTableFilterKey extends MemoizedOperationKey {
        final SelectFilter[] filters;

        TreeTableFilterKey(SelectFilter[] filters) {
            this.filters = filters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final TreeTableFilterKey filter = (TreeTableFilterKey) o;
            return Arrays.equals(filters, filter.filters);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(filters);
        }
    }

    private static final class SwapListenerWithRLL extends ShiftAwareSwapListener {
        private final ReverseLookupListener rll;

        private long rllLastNotificationStep;

        SwapListenerWithRLL(BaseTable sourceTable, ReverseLookupListener rll) {
            super(sourceTable);
            this.rll = rll;
        }

        @Override
        public ConstructSnapshot.SnapshotControl makeSnapshotControl() {
            return ConstructSnapshot.makeSnapshotControl(
                this::startWithRLL,
                (final long currentClockValue,
                    final boolean usingPreviousValues) -> rll
                        .getLastNotificationStep() == rllLastNotificationStep
                        && isInInitialNotificationWindow(),
                (final long afterClockValue,
                    final boolean usedPreviousValues) -> end(afterClockValue));
        }

        @SuppressWarnings("AutoBoxing")
        public synchronized Boolean startWithRLL(final long beforeClockValue) {
            success = false;
            lastNotificationStep = sourceTable.getLastNotificationStep();
            rllLastNotificationStep = rll.getLastNotificationStep();

            final Boolean result;

            final long beforeStep = LogicalClock.getStep(beforeClockValue);
            final LogicalClock.State beforeState = LogicalClock.getState(beforeClockValue);
            if (beforeState == LogicalClock.State.Idle) {
                result = false;
            } else {
                final boolean sourceUpdatedOnThisCycle = lastNotificationStep == beforeStep;
                final boolean rllUpdatedOnThisCycle = rllLastNotificationStep == beforeStep;

                if (sourceUpdatedOnThisCycle) {
                    if (rllUpdatedOnThisCycle || rll.satisfied(beforeStep)) {
                        result = false;
                    } else {
                        WaitNotification.waitForSatisfaction(beforeStep, rll);
                        rllLastNotificationStep = rll.getLastNotificationStep();
                        result = LogicalClock.DEFAULT.currentStep() == beforeStep ? false : null;
                    }
                } else if (rllUpdatedOnThisCycle) {
                    if (sourceTable.satisfied(beforeStep)) {
                        result = false;
                    } else {
                        WaitNotification.waitForSatisfaction(beforeStep, sourceTable);
                        lastNotificationStep = sourceTable.getLastNotificationStep();
                        result = LogicalClock.DEFAULT.currentStep() == beforeStep ? false : null;
                    }
                } else {
                    result = true;
                }
            }
            if (DEBUG) {
                log.info().append("SwapListenerWithRLL start() source=")
                    .append(System.identityHashCode(sourceTable))
                    .append(". swap=")
                    .append(System.identityHashCode(this))
                    .append(", start={").append(beforeStep).append(",")
                    .append(beforeState.toString())
                    .append("}, last=").append(lastNotificationStep)
                    .append(", rllLast=").append(rllLastNotificationStep)
                    .append(", result=").append(result)
                    .endl();
            }
            return result;
        }

        @Override
        public boolean start(final long beforeClockValue) {
            throw new UnsupportedOperationException("Call startWithRLL");
        }

        @Override
        public synchronized boolean end(final long afterClockValue) {
            if (SwapListener.DEBUG) {
                log.info().append("SwapListenerWithRLL end() swap=")
                    .append(System.identityHashCode(this))
                    .append(", end={").append(LogicalClock.getStep(afterClockValue)).append(",")
                    .append(LogicalClock.getState(afterClockValue).toString())
                    .append("}, last=").append(sourceTable.getLastNotificationStep())
                    .append(", rllLast=").append(rll.getLastNotificationStep())
                    .endl();
            }
            return rll.getLastNotificationStep() == rllLastNotificationStep
                && super.end(afterClockValue);
        }

        @Override
        public synchronized void setListenerAndResult(@NotNull final ShiftAwareListener listener,
            @NotNull final NotificationStepReceiver resultTable) {
            super.setListenerAndResult(listener, resultTable);
            if (SwapListener.DEBUG) {
                log.info().append("SwapListenerWithRLL swap=")
                    .append(System.identityHashCode(SwapListenerWithRLL.this)).append(", result=")
                    .append(System.identityHashCode(resultTable)).endl();
            }
        }
    }

    public static Table toTreeTable(Table rawTable, Table originalTree) {
        final Object sourceInfo = originalTree.getAttribute(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
        if (!(sourceInfo instanceof TreeTableInfo)) {
            throw new IllegalArgumentException("Table is not a treeTable");
        }
        final TreeTableInfo treeTableInfo = (TreeTableInfo) sourceInfo;
        return rawTable.treeTable(treeTableInfo.idColumn, treeTableInfo.parentColumn);
    }

    public static Table rawFilterTree(Table tree, String... filters) {
        return rawFilterTree(tree, SelectFilterFactory.getExpressions(filters));
    }

    public static Table rawFilterTree(Table tree, SelectFilter[] filters) {
        return tree.apply(new TreeTableFilter(tree, filters));
    }

    public static Table filterTree(Table tree, String... filters) {
        return filterTree(tree, SelectFilterFactory.getExpressions(filters));
    }

    public static Table filterTree(Table tree, SelectFilter[] filters) {
        return toTreeTable(rawFilterTree(tree, filters), tree);
    }
}

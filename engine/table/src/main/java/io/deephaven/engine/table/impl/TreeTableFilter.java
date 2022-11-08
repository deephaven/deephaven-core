/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.hierarchical.BaseHierarchicalTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.updategraph.WaitNotification;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.LongFunction;

import static io.deephaven.engine.table.Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE;
import static io.deephaven.engine.table.Table.PREPARED_RLL_ATTRIBUTE;

/**
 * Apply filters, preserving parents.
 *
 * The TreeTableFilter takes a TreeTable and WhereFilters as input. The original source table is filtered and any
 * matching rows are included; as well as their ancestors. The result table is then converted into a tree table using
 * the original parameters.
 */
public class TreeTableFilter implements Function<Table, Table>, MemoizedOperationKey.Provider {
    private static final boolean DEBUG = io.deephaven.configuration.Configuration.getInstance()
            .getBooleanWithDefault("TreeTableFilter.debug", false);

    private static final Logger log = LoggerFactory.getLogger(TreeTableFilter.class);

    private final WhereFilter[] filters;
    private final TableDefinition origTableDefinition;

    private TreeTableFilter(Table source, WhereFilter[] filters) {
        this.filters = filters;
        this.origTableDefinition = source.getDefinition();

        for (final WhereFilter filter : filters) {
            filter.init(origTableDefinition);
        }
    }

    @Override
    public Table apply(Table table) {
        return new State(table, origTableDefinition).getRaw();
    }

    @Override
    public MemoizedOperationKey getMemoKey() {
        if (Arrays.stream(filters).allMatch(WhereFilter::canMemoize)) {
            return new TreeTableFilterKey(filters);
        }

        return null;
    }

    private class State {
        private QueryTable filteredRaw;
        private final QueryTable source;

        /**
         * The complete RowSet of our result table.
         */
        private TrackingWritableRowSet resultRowSet;
        /**
         * The RowSet of values that match our desired filter.
         */
        private WritableRowSet valuesRowSet;
        /**
         * The RowSet of ancestors of matching values.
         */
        private WritableRowSet parentRowSet;
        /**
         * For each parent key, a set of rows which directly descend from the parent.
         */
        private Map<Object, TLongSet> parentReferences;

        private final WhereFilter[] filters;
        private final ColumnSource parentSource;
        private final ColumnSource idSource;
        private final ReverseLookup reverseLookupListener;

        @ReferentialIntegrity
        final SwapListenerWithRLL swapListener;

        @ReferentialIntegrity
        TreeTableFilterListener treeListener;

        private final TreeTableInfo treeTableInfo;

        private State(Table table, TableDefinition origTableDefinition) {
            if (!(table instanceof BaseHierarchicalTable)) {
                throw new IllegalArgumentException("Table is not a tree");
            }
            final BaseHierarchicalTable hierarchicalTable = (BaseHierarchicalTable) table;

            source = (QueryTable) hierarchicalTable.getSourceTable();
            final HierarchicalTableInfo sourceInfo = hierarchicalTable.getInfo();
            if (!(sourceInfo instanceof TreeTableInfo)) {
                throw new IllegalArgumentException("Table is not a tree");
            }
            treeTableInfo = (TreeTableInfo) sourceInfo;
            reverseLookupListener =
                    Objects.requireNonNull((ReverseLookupListener) table.getAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE));

            filters = TreeTableFilter.this.filters;

            // The filters have already been inited by here.
            Assert.eq(table.getDefinition(), "Applied table.definition", origTableDefinition, "Original definition");

            parentSource = source.getColumnSource(treeTableInfo.parentColumn);
            idSource = source.getColumnSource(treeTableInfo.idColumn);

            if (source.isRefreshing()) {
                swapListener = new SwapListenerWithRLL(source, reverseLookupListener);
                source.addUpdateListener(swapListener);
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
            valuesRowSet = doValueFilter(usePrev, source.getRowSet());

            parentReferences = new HashMap<>(valuesRowSet.intSize("parentReferenceMap"));
            parentRowSet = computeParents(usePrev, valuesRowSet);
            resultRowSet = valuesRowSet.union(parentRowSet).toTracking();

            validateState(usePrev);

            filteredRaw = (QueryTable) source.getSubTable(resultRowSet);
            if (swapListener != null) {
                treeListener = new TreeTableFilterListener("tree filter", source, filteredRaw);
                swapListener.setListenerAndResult(treeListener, filteredRaw);
                filteredRaw.addParentReference(treeListener);
            }

            // We can re-use the RLL when filtering as long as we are sure to check for existence in the
            // sub table indices. Sticking this annotation here will let QueryTable know it can re-use it.
            filteredRaw.setAttribute(PREPARED_RLL_ATTRIBUTE, reverseLookupListener);
        }

        private Table getRaw() {
            return filteredRaw;
        }

        private void validateState(final boolean usePrev) {
            if (!DEBUG) {
                return;
            }

            final RowSet union = valuesRowSet.union(parentRowSet);

            if (!union.equals(resultRowSet)) {
                throw new IllegalStateException();
            }

            final RowSet expectedRowSet = doValueFilter(usePrev, source.getRowSet());

            final Map<Object, TLongSet> expectedParents = new HashMap<>();

            if (!expectedRowSet.subsetOf(source.getRowSet())) {
                throw new IllegalStateException("Bad refilter!");
            }

            if (!expectedRowSet.equals(valuesRowSet)) {
                final RowSet missing = expectedRowSet.minus(valuesRowSet);
                final RowSet extraValues = valuesRowSet.minus(expectedRowSet);
                throw new IllegalStateException("Inconsistent included Values: missing=" + missing + ", extra="
                        + extraValues + ", expected=" + expectedRowSet + ", valuesRowSet=" + valuesRowSet);
            }

            TLongArrayList parentsToProcess = new TLongArrayList();
            expectedRowSet.forAllRowKeys(parentsToProcess::add);

            final RowSet sourceRowSet = usePrev ? source.getRowSet().copyPrev() : source.getRowSet();
            do {
                final TLongArrayList newParentKeys = new TLongArrayList();
                for (final TLongIterator it = parentsToProcess.iterator(); it.hasNext();) {
                    final long row = it.next();
                    final Object parent = usePrev ? parentSource.getPrev(row) : parentSource.get(row);
                    if (parent == null) {
                        continue;
                    }
                    expectedParents.computeIfAbsent(parent, x -> new TLongHashSet()).add(row);
                    final long parentRow =
                            usePrev ? reverseLookupListener.getPrev(parent) : reverseLookupListener.get(parent);
                    if (parentRow == reverseLookupListener.getNoEntryValue()) {
                        continue;
                    }
                    if (sourceRowSet.find(parentRow) < 0) {
                        throw new IllegalStateException(
                                "Reverse Lookup ShiftObliviousListener points at row " + parentRow + " for "
                                        + parent + ", but the row is not in the rowSet=" + source.getRowSet());
                    }
                    newParentKeys.add(parentRow);
                }
                parentsToProcess = newParentKeys;
            } while (!parentsToProcess.isEmpty());

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();

            parentReferences.forEach((parentValue, set) -> {
                final TLongSet actualSet = parentReferences.get(parentValue);
                final TLongSet expectedSet = expectedParents.get(parentValue);
                if (!actualSet.equals(expectedSet)) {
                    throw new IllegalStateException("Parent set mismatch " + parentValue + ", expected=" + expectedSet
                            + ", actual=" + actualSet);
                }

                final long parentKey =
                        usePrev ? reverseLookupListener.getPrev(parentValue) : reverseLookupListener.get(parentValue);
                if (parentKey != reverseLookupListener.getNoEntryValue()) {
                    // then we should have it in our RowSet
                    builder.addKey(parentKey);
                    final long position = parentRowSet.find(parentKey);
                    if (position < 0) {
                        throw new IllegalStateException(
                                "Could not find parent in our result: " + parentValue + ", key=" + parentKey);
                    }
                }
            });

            final RowSet expectedParentRowSet = builder.build();
            if (!expectedParentRowSet.equals(parentRowSet)) {
                throw new IllegalStateException();
            }
        }

        private void removeValues(RowSet rowsToRemove) {
            valuesRowSet.remove(rowsToRemove);
            removeParents(rowsToRemove);
        }

        private void removeParents(RowSet rowsToRemove) {
            final Map<Object, TLongSet> parents = generateParentReferenceMap(rowsToRemove, parentSource::getPrev);

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator = parents.entrySet().iterator();
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

                            if (valuesRowSet.find(parentKey) < 0) {
                                final Object grandParentId = parentSource.getPrev(parentKey);
                                if (grandParentId != null) {
                                    parents.computeIfAbsent(grandParentId, x -> new TLongHashSet()).add(parentKey);
                                }
                            }
                        }
                    }
                }
            }

            parentRowSet.remove(builder.build());
        }

        private WritableRowSet doValueFilter(boolean usePrev, RowSet rowsToFilter) {
            WritableRowSet matched = rowsToFilter.copy();
            for (final WhereFilter filter : filters) {
                try (final SafeCloseable ignored = matched) { // Ensure we close old matched
                    matched = filter.filter(matched, source.getRowSet(), source, usePrev);
                }
            }
            return matched;
        }

        private RowSet checkForResurrectedParent(RowSet rowsToCheck) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

            for (final RowSet.Iterator it = rowsToCheck.iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final Object id = idSource.get(key);

                if (parentReferences.containsKey(id)) {
                    builder.appendKey(key);
                }
            }

            return builder.build();
        }

        private WritableRowSet computeParents(final boolean usePrev, @NotNull final RowSet rowsToParent) {
            final Map<Object, TLongSet> parents =
                    generateParentReferenceMap(rowsToParent, usePrev ? parentSource::getPrev : parentSource::get);

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator = parents.entrySet().iterator();
                final Map.Entry<Object, TLongSet> entry = iterator.next();
                final Object parent = entry.getKey();
                final TLongSet references = entry.getValue();
                iterator.remove();

                final long parentKey =
                        usePrev ? reverseLookupListener.getPrev(parent) : reverseLookupListener.get(parent);
                if (parentKey != reverseLookupListener.getNoEntryValue()) {
                    builder.addKey(parentKey);
                    final Object grandParentId =
                            usePrev ? parentSource.getPrev(parentKey) : parentSource.get(parentKey);
                    if (grandParentId != null) {
                        parents.computeIfAbsent(grandParentId, x -> new TLongHashSet()).add(parentKey);
                    }
                }

                parentReferences.computeIfAbsent(parent, x -> new TLongHashSet()).addAll(references);
            }

            return builder.build();
        }

        @NotNull
        private Map<Object, TLongSet> generateParentReferenceMap(RowSet rowsToParent, LongFunction<Object> getValue) {
            final Map<Object, TLongSet> parents = new LinkedHashMap<>(rowsToParent.intSize());
            for (final RowSet.Iterator it = rowsToParent.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object parentId = getValue.apply(row);
                if (parentId != null) {
                    parents.computeIfAbsent(parentId, (x) -> new TLongHashSet()).add(row);
                }
            }
            return parents;
        }

        private class TreeTableFilterListener extends BaseTable.ListenerImpl {
            final ModifiedColumnSet inputColumns;

            TreeTableFilterListener(String description, Table parent, QueryTable dependent) {
                super(description, parent, dependent);
                inputColumns = source.newModifiedColumnSet(treeTableInfo.idColumn, treeTableInfo.parentColumn);
                Arrays.stream(filters).forEach(filter -> {
                    for (final String column : filter.getColumns()) {
                        inputColumns.setAll(column);
                    }
                });
            }

            @Override
            public void onUpdate(final TableUpdate upstream) {
                // TODO: make chunk happy

                final TableUpdateImpl downstream = new TableUpdateImpl();

                final long rllLastStep = reverseLookupListener.getLastNotificationStep();
                final long sourceLastStep = source.getLastNotificationStep();

                if (rllLastStep != sourceLastStep) {
                    throw new IllegalStateException(
                            "RLL was updated in a different cycle! Rll: " + rllLastStep + " source: " + sourceLastStep);
                }

                // We can ignore modified while updating if columns we care about were not touched.
                final boolean useModified = upstream.modifiedColumnSet().containsAny(inputColumns);

                // Must take care of removed here, because these rows are not valid in post shift space.
                downstream.removed = resultRowSet.extract(upstream.removed());

                try (final RowSet allRemoved =
                        useModified ? upstream.removed().union(upstream.getModifiedPreShift()) : null;
                        final RowSet valuesToRemove =
                                (useModified ? allRemoved : upstream.removed()).intersect(valuesRowSet);
                        final RowSet removedParents =
                                (useModified ? allRemoved : upstream.removed()).intersect(parentRowSet)) {

                    removeValues(valuesToRemove);
                    parentRowSet.remove(removedParents);
                    removeParents(removedParents);
                }

                // Now we must shift all maintained state.
                upstream.shifted().forAllInRowSet(resultRowSet, (key, delta) -> {
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
                upstream.shifted().apply(valuesRowSet);
                upstream.shifted().apply(parentRowSet);
                upstream.shifted().apply(resultRowSet);

                // Finally, handle added sets.
                try (final WritableRowSet addedAndModified = upstream.added().union(upstream.modified());
                        final RowSet newFiltered = doValueFilter(false, addedAndModified);
                        final RowSet resurrectedParents = checkForResurrectedParent(addedAndModified);
                        final RowSet newParents = computeParents(false, newFiltered);
                        final RowSet newResurrectedParents = computeParents(false, resurrectedParents)) {


                    valuesRowSet.insert(newFiltered);
                    parentRowSet.insert(newParents);
                    parentRowSet.insert(resurrectedParents);
                    parentRowSet.insert(newResurrectedParents);
                }

                // Compute expected results and the sets we will propagate to child listeners.
                try (final RowSet result = valuesRowSet.union(parentRowSet);
                        final WritableRowSet resultRemovals = resultRowSet.minus(result)) {
                    downstream.added = result.minus(resultRowSet);
                    resultRowSet.update(downstream.added(), resultRemovals);

                    downstream.modified = upstream.modified().intersect(resultRowSet);
                    downstream.modified().writableCast().remove(downstream.added());

                    // convert post filter removals into pre-shift space -- note these rows must have previously existed
                    upstream.shifted().unapply(resultRemovals);
                    downstream.removed().writableCast().insert(resultRemovals);
                }

                downstream.shifted = upstream.shifted();
                downstream.modifiedColumnSet = upstream.modifiedColumnSet(); // note that dependent is a subTable

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
        final WhereFilter[] filters;

        TreeTableFilterKey(WhereFilter[] filters) {
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

    private static final class SwapListenerWithRLL extends SwapListener {
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
                    (final long afterClockValue, final boolean usedPreviousValues) -> end(afterClockValue));
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
                        .append(", start={").append(beforeStep).append(",").append(beforeState.toString())
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
                log.info().append("SwapListenerWithRLL end() swap=").append(System.identityHashCode(this))
                        .append(", end={").append(LogicalClock.getStep(afterClockValue)).append(",")
                        .append(LogicalClock.getState(afterClockValue).toString())
                        .append("}, last=").append(sourceTable.getLastNotificationStep())
                        .append(", rllLast=").append(rll.getLastNotificationStep())
                        .endl();
            }
            return rll.getLastNotificationStep() == rllLastNotificationStep && super.end(afterClockValue);
        }

        @Override
        public synchronized void setListenerAndResult(@NotNull final TableUpdateListener listener,
                @NotNull final NotificationStepReceiver resultTable) {
            super.setListenerAndResult(listener, resultTable);
            if (SwapListener.DEBUG) {
                log.info().append("SwapListenerWithRLL swap=").append(System.identityHashCode(SwapListenerWithRLL.this))
                        .append(", result=").append(System.identityHashCode(resultTable)).endl();
            }
        }
    }

    public static Table toTreeTable(Table rawTable, Table originalTree) {
        final Object sourceInfo = originalTree.getAttribute(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
        if (!(sourceInfo instanceof TreeTableInfo)) {
            throw new IllegalArgumentException("Table is not a tree");
        }
        final TreeTableInfo treeTableInfo = (TreeTableInfo) sourceInfo;
        return rawTable.tree(treeTableInfo.idColumn, treeTableInfo.parentColumn);
    }

    public static Table rawFilterTree(Table tree, String... filters) {
        return rawFilterTree(tree, WhereFilterFactory.getExpressions(filters));
    }

    public static Table rawFilterTree(Table tree, WhereFilter[] filters) {
        return tree.apply(new TreeTableFilter(tree, filters));
    }

    public static Table filterTree(Table tree, String... filters) {
        return filterTree(tree, WhereFilterFactory.getExpressions(filters));
    }

    public static Table filterTree(Table tree, WhereFilter[] filters) {
        return toTreeTable(rawFilterTree(tree, filters), tree);
    }
}

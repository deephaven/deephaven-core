/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl.TreeReverseLookup;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
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

/**
 * Apply filters to a tree source table, preserving ancestors.
 * <p>
 * The TreeTableFilter takes a {@link TreeTableImpl tree} and {@link WhereFilter fllters} as input, and expects to be
 * {@link Table#apply(Function) applied} to the tree's {@link HierarchicalTableImpl#getSource() source}. Applying the
 * filter will produce a new table intended to be the source for a subsequent tree operation with the same parameters as
 * the input. The result table includes any rows matched by the input filters, as well as all ancestors of those rows.
 */
class TreeTableFilter implements Function<Table, Table>, MemoizedOperationKey.Provider {

    private static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("TreeTableFilter.debug", false);

    private static final Logger log = LoggerFactory.getLogger(TreeTableFilter.class);

    private final TreeTableImpl tree;
    private final WhereFilter[] filters;

    TreeTableFilter(@NotNull final TreeTableImpl tree, @NotNull final WhereFilter[] filters) {
        this.tree = tree;
        this.filters = filters; // NB: The tree will always have initialized these filters ahead of time
    }

    @ConcurrentMethod
    @Override
    public Table apply(@NotNull final Table table) {
        Require.eq(table, "table", tree.getSource(), "tree.getSource()");
        return new State(tree, filters).getResult();
    }

    @Override
    public MemoizedOperationKey getMemoKey() {
        if (Arrays.stream(filters).allMatch(WhereFilter::canMemoize)) {
            return new TreeTableFilterKey(filters);
        }
        return null;
    }

    private static class State {

        /**
         * The source table of the {@link TreeTableImpl} to be filtered.
         */
        private final QueryTable source;

        /**
         * The rpw identifier column name from the {@link TreeTableImpl} to be filtered.
         */
        private final ColumnName idColumnName;

        /**
         * The parent identifier column name from the {@link TreeTableImpl} to be filtered.
         */
        private final ColumnName parentIdColumnName;

        /**
         * The input {@link TreeTableImpl tree's} {@link TreeReverseLookup reverse lookup}.
         */
        private final TreeReverseLookup reverseLookup;

        /**
         * The (initialized) filters to apply to {@link #source} in order to produce {@link #matched}.
         */
        private final WhereFilter[] filters;

        /**
         * The row identifier source from {@link #source}.
         */
        private final ColumnSource idSource;

        /**
         * The parent identifier source from {@link #source}.
         */
        private final ColumnSource parentIdSource;

        /**
         * Swap listener for concurrent instantiation.
         */
        @ReferentialIntegrity
        private final SwapListenerEx swapListener;

        /**
         * The eventual listener that maintains {@link #result}.
         */
        @ReferentialIntegrity
        private Listener sourceListener;

        /**
         * The result table after filtering {@link #source} and re-adding ancestors.
         */
        private QueryTable result;

        /**
         * The complete RowSet of our result table.
         */
        private TrackingWritableRowSet resultRowSet;

        /**
         * The rows from {@link #source} that match our filters.
         */
        private WritableRowSet matched;

        /**
         * The rows from {@link #source} containing all ancestors of matched rows.
         */
        private WritableRowSet ancestors;

        /**
         * For each parent key, a set of rows which directly descend from the parent.
         */
        private Map<Object, TLongSet> parentReferences;

        private State(@NotNull final TreeTableImpl tree, @NotNull final WhereFilter[] filters) {
            source = (QueryTable) tree.getSource();
            idColumnName = tree.getIdentifierColumn();
            parentIdColumnName = tree.getParentIdentifierColumn();
            reverseLookup = tree.getReverseLookup();
            this.filters = filters;

            idSource = source.getColumnSource(tree.getIdentifierColumn().name());
            parentIdSource = source.getColumnSource(tree.getParentIdentifierColumn().name());

            if (source.isRefreshing()) {
                swapListener = new SwapListenerEx(source, reverseLookup);
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
            matched = doValueFilter(usePrev, source.getRowSet());

            parentReferences = new HashMap<>(matched.intSize("parentReferenceMap"));
            ancestors = computeParents(usePrev, matched);
            resultRowSet = matched.union(ancestors).toTracking();

            validateState(usePrev);

            result = source.getSubTable(resultRowSet);
            if (swapListener != null) {
                sourceListener = new Listener("tree filter", source, result);
                swapListener.setListenerAndResult(sourceListener, result);
                result.addParentReference(sourceListener);
            }
        }

        private Table getResult() {
            return result;
        }

        private void validateState(final boolean usePrev) {
            if (!DEBUG) {
                return;
            }

            final RowSet union = matched.union(ancestors);

            if (!union.equals(resultRowSet)) {
                throw new IllegalStateException();
            }

            final RowSet expectedRowSet = doValueFilter(usePrev, source.getRowSet());

            final Map<Object, TLongSet> expectedParents = new HashMap<>();

            if (!expectedRowSet.subsetOf(source.getRowSet())) {
                throw new IllegalStateException("Bad refilter!");
            }

            if (!expectedRowSet.equals(matched)) {
                final RowSet missing = expectedRowSet.minus(matched);
                final RowSet extraValues = matched.minus(expectedRowSet);
                throw new IllegalStateException("Inconsistent included Values: missing=" + missing + ", extra="
                        + extraValues + ", expected=" + expectedRowSet + ", valuesRowSet=" + matched);
            }

            TLongArrayList parentsToProcess = new TLongArrayList();
            expectedRowSet.forAllRowKeys(parentsToProcess::add);

            final RowSet sourceRowSet = usePrev ? source.getRowSet().copyPrev() : source.getRowSet();
            do {
                final TLongArrayList newParentKeys = new TLongArrayList();
                for (final TLongIterator it = parentsToProcess.iterator(); it.hasNext();) {
                    final long row = it.next();
                    final Object parent = usePrev ? parentIdSource.getPrev(row) : parentIdSource.get(row);
                    if (parent == null) {
                        continue;
                    }
                    expectedParents.computeIfAbsent(parent, x -> new TLongHashSet()).add(row);
                    final long parentRow =
                            usePrev ? reverseLookup.getPrev(parent) : reverseLookup.get(parent);
                    if (parentRow == reverseLookup.noEntryValue()) {
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
                        usePrev ? reverseLookup.getPrev(parentValue) : reverseLookup.get(parentValue);
                if (parentKey != reverseLookup.noEntryValue()) {
                    // then we should have it in our RowSet
                    builder.addKey(parentKey);
                    final long position = ancestors.find(parentKey);
                    if (position < 0) {
                        throw new IllegalStateException(
                                "Could not find parent in our result: " + parentValue + ", key=" + parentKey);
                    }
                }
            });

            final RowSet expectedParentRowSet = builder.build();
            if (!expectedParentRowSet.equals(ancestors)) {
                throw new IllegalStateException();
            }
        }

        private void removeValues(RowSet rowsToRemove) {
            matched.remove(rowsToRemove);
            removeParents(rowsToRemove);
        }

        private void removeParents(RowSet rowsToRemove) {
            final Map<Object, TLongSet> parents = generateParentReferenceMap(rowsToRemove, parentIdSource::getPrev);

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator = parents.entrySet().iterator();
                final Map.Entry<Object, TLongSet> entry = iterator.next();
                final Object parent = entry.getKey();
                final TLongSet references = entry.getValue();
                iterator.remove();

                final long parentKey = reverseLookup.getPrev(parent);

                final TLongSet parentSet = parentReferences.get(parent);
                if (parentSet != null) {
                    parentSet.removeAll(references);

                    if (parentSet.isEmpty()) {
                        parentReferences.remove(parent);
                        if (parentKey != reverseLookup.noEntryValue()) {
                            builder.addKey(parentKey);

                            if (matched.find(parentKey) < 0) {
                                final Object grandParentId = parentIdSource.getPrev(parentKey);
                                if (grandParentId != null) {
                                    parents.computeIfAbsent(grandParentId, x -> new TLongHashSet()).add(parentKey);
                                }
                            }
                        }
                    }
                }
            }

            ancestors.remove(builder.build());
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
                    generateParentReferenceMap(rowsToParent, usePrev ? parentIdSource::getPrev : parentIdSource::get);

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            while (!parents.isEmpty()) {
                final Iterator<Map.Entry<Object, TLongSet>> iterator = parents.entrySet().iterator();
                final Map.Entry<Object, TLongSet> entry = iterator.next();
                final Object parent = entry.getKey();
                final TLongSet references = entry.getValue();
                iterator.remove();

                final long parentKey =
                        usePrev ? reverseLookup.getPrev(parent) : reverseLookup.get(parent);
                if (parentKey != reverseLookup.noEntryValue()) {
                    builder.addKey(parentKey);
                    final Object grandParentId =
                            usePrev ? parentIdSource.getPrev(parentKey) : parentIdSource.get(parentKey);
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

        private class Listener extends BaseTable.ListenerImpl {

            final ModifiedColumnSet inputColumns;

            Listener(String description, Table parent, QueryTable dependent) {
                super(description, parent, dependent);
                inputColumns = source.newModifiedColumnSet(idColumnName.name(), parentIdColumnName.name());
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

                final long rllLastStep = reverseLookup.getLastNotificationStep();
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
                                (useModified ? allRemoved : upstream.removed()).intersect(matched);
                        final RowSet removedParents =
                                (useModified ? allRemoved : upstream.removed()).intersect(ancestors)) {

                    removeValues(valuesToRemove);
                    ancestors.remove(removedParents);
                    removeParents(removedParents);
                }

                // Now we must shift all maintained state.
                upstream.shifted().forAllInRowSet(resultRowSet, (key, delta) -> {
                    final Object parentId = parentIdSource.getPrev(key);
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
                upstream.shifted().apply(matched);
                upstream.shifted().apply(ancestors);
                upstream.shifted().apply(resultRowSet);

                // Finally, handle added sets.
                try (final WritableRowSet addedAndModified = upstream.added().union(upstream.modified());
                        final RowSet newFiltered = doValueFilter(false, addedAndModified);
                        final RowSet resurrectedParents = checkForResurrectedParent(addedAndModified);
                        final RowSet newParents = computeParents(false, newFiltered);
                        final RowSet newResurrectedParents = computeParents(false, resurrectedParents)) {


                    matched.insert(newFiltered);
                    ancestors.insert(newParents);
                    ancestors.insert(resurrectedParents);
                    ancestors.insert(newResurrectedParents);
                }

                // Compute expected results and the sets we will propagate to child listeners.
                try (final RowSet result = matched.union(ancestors);
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

                result.notifyListeners(downstream);

                validateState(false);
            }

            @Override
            public boolean canExecute(final long step) {
                return super.canExecute(step) && reverseLookup.satisfied(step);
            }
        }
    }

    private static class TreeTableFilterKey extends MemoizedOperationKey {

        private final WhereFilter[] filters;

        private TreeTableFilterKey(@NotNull final WhereFilter[] filters) {
            this.filters = filters;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final TreeTableFilterKey otherKey = (TreeTableFilterKey) other;
            return Arrays.equals(filters, otherKey.filters);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(filters);
        }
    }
}

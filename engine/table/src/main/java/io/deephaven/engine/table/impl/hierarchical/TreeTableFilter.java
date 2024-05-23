//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Apply filters to a tree source table, preserving ancestors.
 * <p>
 * The TreeTableFilter is created by an {@link Operator} that takes a {@link TreeTableImpl tree table} and
 * {@link WhereFilter fllters} as input, and expects to be {@link Table#apply(Function) applied} to the tree table's
 * {@link HierarchicalTableImpl#getSource() source}. Applying the filter will produce a new table intended to be the
 * source for a subsequent tree operation with the same parameters as the input. The result table includes any rows
 * matched by the input filters, as well as all ancestors of those rows.
 */
@VisibleForTesting
public class TreeTableFilter {

    private static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("TreeTableFilter.debug", false);
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    @VisibleForTesting
    public static final class Operator implements Function<Table, Table>, MemoizedOperationKey.Provider {

        private final TreeTableImpl treeTable;
        private final WhereFilter[] filters;

        @VisibleForTesting
        public Operator(@NotNull final TreeTableImpl treeTable, @NotNull final WhereFilter[] filters) {
            this.treeTable = treeTable;
            this.filters = filters; // NB: The tree will always have initialized these filters ahead of time
        }

        @ConcurrentMethod
        @Override
        public Table apply(@NotNull final Table table) {
            Require.eq(table, "table", treeTable.getSource(), "tree.getSource()");
            return new TreeTableFilter(treeTable, filters).getResult();
        }

        @Override
        public MemoizedOperationKey getMemoKey() {
            if (Arrays.stream(filters).allMatch(WhereFilter::canMemoize)) {
                return new TreeTableFilterKey(filters);
            }
            return null;
        }
    }

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
     * The input {@link TreeTableImpl tree's} {@link TreeSourceRowLookup source row lookup}.
     */
    private final TreeSourceRowLookup sourceRowLookup;

    /**
     * The (initialized) filters to apply to {@link #source} in order to produce {@link #matchedSourceRows}.
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
     * The eventual listener that maintains {@link #result}.
     */
    @SuppressWarnings("FieldCanBeLocal")
    @ReferentialIntegrity
    private Listener sourceListener;

    /**
     * The result table after filtering {@link #source} and re-adding ancestors.
     */
    private QueryTable result;

    /**
     * The complete RowSet of our result table.
     */
    private TrackingWritableRowSet resultRows;

    /**
     * The rows from {@link #source} that match our filters.
     */
    private WritableRowSet matchedSourceRows;

    /**
     * The rows from {@link #source} containing all ancestors of matched rows.
     */
    private WritableRowSet ancestorSourceRows;

    /**
     * For each included parent identifier value, the row keys from {@link #matchedSourceRows} or
     * {@link #ancestorSourceRows} which directly descend from the parent.
     */
    private Map<Object, WritableRowSet> parentIdToChildRows;

    private TreeTableFilter(@NotNull final TreeTableImpl tree, @NotNull final WhereFilter[] filters) {
        source = (QueryTable) tree.getSource();
        idColumnName = tree.getIdentifierColumn();
        parentIdColumnName = tree.getParentIdentifierColumn();
        sourceRowLookup = tree.getSourceRowLookup();
        this.filters = filters;
        final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor = QueryCompilerRequestProcessor.batch();
        Arrays.stream(filters)
                .forEach((final WhereFilter filter) -> filter.init(source.getDefinition(), compilationProcessor));
        compilationProcessor.compile();

        idSource = source.getColumnSource(tree.getIdentifierColumn().name());
        parentIdSource = source.getColumnSource(tree.getParentIdentifierColumn().name());

        if (source.isRefreshing()) {
            try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(
                    source.getUpdateGraph()).open()) {
                final OperationSnapshotControlEx snapshotControl =
                        new OperationSnapshotControlEx(source, sourceRowLookup);
                ConstructSnapshot.callDataSnapshotFunction(System.identityHashCode(source) + ": ",
                        snapshotControl,
                        (usePrev, beforeClockValue) -> {
                            doInitialFilter(snapshotControl, usePrev);
                            return true;
                        });
            }
        } else {
            doInitialFilter(null, false);
        }
    }

    private void doInitialFilter(@Nullable final OperationSnapshotControl snapshotControl, final boolean usePrev) {
        try (final RowSet sourcePrevRows = usePrev ? source.getRowSet().copyPrev() : null) {
            final RowSet sourceRows = usePrev ? sourcePrevRows : source.getRowSet();

            matchedSourceRows = filterValues(usePrev, sourceRows, sourceRows);
            parentIdToChildRows = new HashMap<>(matchedSourceRows.intSize("parentReferences"));
            ancestorSourceRows = computeParents(usePrev, matchedSourceRows);
            resultRows = matchedSourceRows.union(ancestorSourceRows).toTracking();

            validateState(usePrev, sourceRows);
        }

        result = source.getSubTable(resultRows);
        if (snapshotControl != null) {
            sourceListener = new Listener();
            snapshotControl.setListenerAndResult(sourceListener, result);
            result.addParentReference(sourceListener);
        }
    }

    private Table getResult() {
        return result;
    }

    private void validateState(final boolean usePrev, @NotNull final RowSet sourceRows) {
        if (!DEBUG) {
            return;
        }

        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSet allIncludedRows = toClose.add(matchedSourceRows.union(ancestorSourceRows));
            if (!allIncludedRows.equals(resultRows)) {
                throw new IllegalStateException();
            }

            final RowSet expectedMatches = toClose.add(filterValues(usePrev, sourceRows, sourceRows));
            if (!expectedMatches.subsetOf(sourceRows)) {
                throw new IllegalStateException("Filtering bug: matches=" + expectedMatches
                        + " are not a subset of source rows=" + sourceRows);
            }

            if (!expectedMatches.equals(matchedSourceRows)) {
                final RowSet missingMatches = toClose.add(expectedMatches.minus(matchedSourceRows));
                final RowSet extraMatches = toClose.add(matchedSourceRows.minus(expectedMatches));
                throw new IllegalStateException("Inconsistent matched values: missing=" + missingMatches
                        + ", extra=" + extraMatches
                        + ", expected=" + expectedMatches
                        + ", actual=" + matchedSourceRows);
            }

            final Map<Object, RowSetBuilderRandom> expectedParents = new HashMap<>();
            RowSet childRowsToProcess = expectedMatches;
            try (final ChunkSource.GetContext parentIdGetContext = parentIdSource.makeGetContext(CHUNK_SIZE);
                    final ChunkBoxer.BoxerKernel boxer =
                            ChunkBoxer.getBoxer(parentIdSource.getChunkType(), CHUNK_SIZE)) {
                final MutableInt chunkOffset = new MutableInt();
                while (!childRowsToProcess.isEmpty()) {
                    final RowSetBuilderRandom newParentKeys = RowSetFactory.builderRandom();
                    try (final SafeCloseable ignored =
                            childRowsToProcess == expectedMatches ? null : childRowsToProcess;
                            final RowSequence.Iterator childRowsToProcessIter =
                                    childRowsToProcess.getRowSequenceIterator()) {
                        while (childRowsToProcessIter.hasMore()) {
                            final RowSequence chunkChildRows =
                                    childRowsToProcessIter.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final ObjectChunk<?, ? extends Values> parentIds =
                                    getIds(usePrev, parentIdSource, parentIdGetContext, boxer, chunkChildRows);
                            chunkOffset.setValue(0);
                            chunkChildRows.forAllRowKeys((final long childRow) -> {
                                final Object parentId = parentIds.get(chunkOffset.getAndIncrement());
                                expectedParents.computeIfAbsent(parentId, pid -> RowSetFactory.builderRandom())
                                        .addKey(childRow);
                                final long parentRow =
                                        usePrev ? sourceRowLookup.getPrev(parentId) : sourceRowLookup.get(parentId);
                                if (parentRow == sourceRowLookup.noEntryValue()) {
                                    return;
                                }
                                if (sourceRows.find(parentRow) < 0) {
                                    throw new IllegalStateException("Source row lookup lookup points at row "
                                            + parentRow + " for " + parentId
                                            + ", but the row is not in the source rows=" + sourceRows);
                                }
                                newParentKeys.addKey(parentRow);
                            });
                        }
                    }
                    childRowsToProcess = newParentKeys.build();
                }
            }
            if (childRowsToProcess != expectedMatches) {
                childRowsToProcess.close();
            }

            final RowSetBuilderRandom expectedAncestorRowsBuilder = RowSetFactory.builderRandom();
            parentIdToChildRows.forEach((final Object parentId, final RowSet actualRows) -> {
                try (final RowSet expectedRows = expectedParents.get(parentId).build()) {
                    if (!actualRows.equals(expectedRows)) {
                        throw new IllegalStateException("Parent rows mismatch for id=" + parentId
                                + ", expected=" + expectedRows
                                + ", actual=" + actualRows);
                    }

                    final long parentRow = usePrev ? sourceRowLookup.getPrev(parentId) : sourceRowLookup.get(parentId);
                    if (parentRow != sourceRowLookup.noEntryValue()) {
                        expectedAncestorRowsBuilder.addKey(parentRow);
                        final long parentRowPosition = ancestorSourceRows.find(parentRow);
                        if (parentRowPosition < 0) {
                            throw new IllegalStateException(
                                    "Could not find parent in our result: id=" + parentId + ", row=" + parentRow);
                        }
                    }
                }
            });

            final RowSet expectedAncestorRows = toClose.add(expectedAncestorRowsBuilder.build());
            if (!expectedAncestorRows.equals(ancestorSourceRows)) {
                throw new IllegalStateException();
            }
        }
    }

    private static ObjectChunk<?, ? extends Values> getIds(
            final boolean usePrev,
            @NotNull final ColumnSource<?> idChunkSource,
            @NotNull final ChunkSource.GetContext idSourceGetContext,
            @NotNull final ChunkBoxer.BoxerKernel boxer,
            @NotNull final RowSequence chunkChildRows) {
        final Chunk<? extends Values> unboxedParentIds =
                usePrev ? idChunkSource.getPrevChunk(idSourceGetContext, chunkChildRows)
                        : idChunkSource.getChunk(idSourceGetContext, chunkChildRows);
        return boxer.box(unboxedParentIds);
    }

    private void removeValues(@NotNull final RowSet rowsToRemove) {
        matchedSourceRows.remove(rowsToRemove);
        removeParents(rowsToRemove);
    }

    private void removeParents(@NotNull final RowSet rowsToRemove) {
        final RowSetBuilderRandom removedAncestorsBuilder = RowSetFactory.builderRandom();

        Map<Object, RowSetBuilderSequential> bucketed = bucketChildRowsByParentId(true, rowsToRemove);
        while (!bucketed.isEmpty()) {
            final RowSetBuilderRandom levelRemovedAncestorsBuilder = RowSetFactory.builderRandom();
            bucketed.forEach((final Object parentId, final RowSetBuilderSequential levelChildRowsForParent) -> {
                final WritableRowSet existingChildRows = parentIdToChildRows.get(parentId);
                if (existingChildRows == null) {
                    return;
                }
                try (final RowSet removedChildRows = levelChildRowsForParent.build()) {
                    existingChildRows.remove(removedChildRows);
                }
                if (existingChildRows.isNonempty()) {
                    return;
                }
                parentIdToChildRows.remove(parentId).close();
                final long parentRow = sourceRowLookup.getPrev(parentId);
                if (parentRow == sourceRowLookup.noEntryValue()) {
                    return;
                }
                levelRemovedAncestorsBuilder.addKey(parentRow);
            });
            try (final WritableRowSet levelRemovedAncestorRows = levelRemovedAncestorsBuilder.build()) {
                removedAncestorsBuilder.addRowSet(levelRemovedAncestorRows);
                levelRemovedAncestorRows.remove(matchedSourceRows);
                bucketed = bucketChildRowsByParentId(true, levelRemovedAncestorRows);
            }
        }

        ancestorSourceRows.remove(removedAncestorsBuilder.build());
    }

    private WritableRowSet filterValues(
            final boolean usePrev,
            @NotNull final RowSet allSourceRows,
            @NotNull final RowSet sourceRowsToFilter) {
        WritableRowSet matchedRows = sourceRowsToFilter.copy();
        for (final WhereFilter filter : filters) {
            try (final SafeCloseable ignored = matchedRows) { // Ensure we close old matchedRows
                matchedRows = filter.filter(matchedRows, allSourceRows, source, usePrev);
            }
        }
        return matchedRows;
    }

    private RowSet checkForResurrectedParent(@NotNull final RowSet rowsToCheck) {
        final RowSetBuilderSequential rowsToReParent = RowSetFactory.builderSequential();

        try (final ChunkSource.GetContext idGetContext = idSource.makeGetContext(CHUNK_SIZE);
                final ChunkBoxer.BoxerKernel boxer = ChunkBoxer.getBoxer(idSource.getChunkType(), CHUNK_SIZE);
                final RowSequence.Iterator rowsToCheckIter = rowsToCheck.getRowSequenceIterator()) {
            final MutableInt chunkOffset = new MutableInt();
            while (rowsToCheckIter.hasMore()) {
                final RowSequence chunkRowsToCheck = rowsToCheckIter.getNextRowSequenceWithLength(CHUNK_SIZE);
                final ObjectChunk<?, ? extends Values> ids =
                        getIds(false, idSource, idGetContext, boxer, chunkRowsToCheck);
                chunkOffset.setValue(0);
                chunkRowsToCheck.forAllRowKeys((final long rowKeyToCheck) -> {
                    final Object id = ids.get(chunkOffset.getAndIncrement());
                    if (id != null && parentIdToChildRows.containsKey(id)) {
                        rowsToReParent.appendKey(rowKeyToCheck);
                    }
                });
            }
        }

        return rowsToReParent.build();
    }

    private WritableRowSet computeParents(final boolean usePrev, @NotNull final RowSet rowsToParent) {
        final RowSetBuilderRandom includedParentRowsBuilder = RowSetFactory.builderRandom();

        Map<Object, RowSetBuilderSequential> bucketed = bucketChildRowsByParentId(usePrev, rowsToParent);
        while (!bucketed.isEmpty()) {
            final RowSetBuilderRandom levelIncludedParentRowsBuilder = RowSetFactory.builderRandom();
            bucketed.forEach((final Object parentId, final RowSetBuilderSequential levelChildRowsForParent) -> {
                final long parentRow = usePrev ? sourceRowLookup.getPrev(parentId) : sourceRowLookup.get(parentId);
                if (parentRow != sourceRowLookup.noEntryValue()) {
                    levelIncludedParentRowsBuilder.addKey(parentRow);
                }
                parentIdToChildRows.merge(parentId, levelChildRowsForParent.build(),
                        TreeTableFilter::accumulateChildRows);
            });
            try (final RowSet levelIncludedParentRows = levelIncludedParentRowsBuilder.build()) {
                includedParentRowsBuilder.addRowSet(levelIncludedParentRows);
                bucketed = bucketChildRowsByParentId(usePrev, levelIncludedParentRows);
            }
        }

        return includedParentRowsBuilder.build();
    }

    private static WritableRowSet accumulateChildRows(
            @NotNull final WritableRowSet existingChildRows,
            @NotNull final WritableRowSet childRowsToInsert) {
        try (final SafeCloseable ignored = childRowsToInsert) {
            existingChildRows.insert(childRowsToInsert);
        }
        return existingChildRows;
    }

    private Map<Object, RowSetBuilderSequential> bucketChildRowsByParentId(
            final boolean usePrev,
            @NotNull final RowSequence rowsToParent) {
        if (rowsToParent.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Object, RowSetBuilderSequential> parentIdToChildRows =
                new LinkedHashMap<>(rowsToParent.intSize());
        try (final ChunkSource.GetContext parentIdGetContext = parentIdSource.makeGetContext(CHUNK_SIZE);
                final ChunkBoxer.BoxerKernel boxer = ChunkBoxer.getBoxer(parentIdSource.getChunkType(), CHUNK_SIZE);
                final RowSequence.Iterator childRowsIter = rowsToParent.getRowSequenceIterator()) {
            final MutableInt chunkOffset = new MutableInt();
            while (childRowsIter.hasMore()) {
                final RowSequence chunkChildRows = childRowsIter.getNextRowSequenceWithLength(CHUNK_SIZE);
                final ObjectChunk<?, ? extends Values> parentIds =
                        getIds(usePrev, parentIdSource, parentIdGetContext, boxer, chunkChildRows);
                chunkOffset.setValue(0);
                chunkChildRows.forAllRowKeys((final long childRowKey) -> {
                    final Object parentId = parentIds.get(chunkOffset.getAndIncrement());
                    if (parentId != null) {
                        parentIdToChildRows.computeIfAbsent(parentId,
                                pid -> RowSetFactory.builderSequential()).appendKey(childRowKey);
                    }
                });
            }
        }
        return parentIdToChildRows;
    }

    private void shiftParentIdToChildRows(@NotNull final RowSetShiftData upstreamShifts) {
        // Map from parent identifier to a pair of removed, added row set builders.
        final Map<Object, Pair<RowSetBuilderSequential, RowSetBuilderSequential>> affectedParents = new HashMap<>();
        try (final ChunkSource.GetContext parentIdGetContext = parentIdSource.makeGetContext(CHUNK_SIZE);
                final ChunkBoxer.BoxerKernel boxer =
                        ChunkBoxer.getBoxer(parentIdSource.getChunkType(), CHUNK_SIZE)) {
            final int numShifts = upstreamShifts.size();
            for (int si = 0; si < numShifts; ++si) {
                final long beginRange = upstreamShifts.getBeginRange(si);
                final long endRange = upstreamShifts.getEndRange(si);
                final long shiftDelta = upstreamShifts.getShiftDelta(si);
                try (final RowSequence.Iterator rowsIter = resultRows.getRowSequenceIterator()) {
                    rowsIter.advance(beginRange);
                    final RowSequence affectedRows = rowsIter.getNextRowSequenceThrough(endRange);
                    try (final RowSequence.Iterator affectedRowsIter = affectedRows.getRowSequenceIterator()) {
                        while (affectedRowsIter.hasMore()) {
                            final RowSequence chunkAffectedRows =
                                    affectedRowsIter.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final ObjectChunk<?, ? extends Values> parentIds =
                                    getIds(true, parentIdSource, parentIdGetContext, boxer, chunkAffectedRows);
                            final MutableInt chunkIndex = new MutableInt(0);
                            chunkAffectedRows.forAllRowKeys((final long affectedRowKey) -> {
                                final Object parentId = parentIds.get(chunkIndex.getAndIncrement());
                                final Pair<RowSetBuilderSequential, RowSetBuilderSequential> removedAddedPair =
                                        affectedParents.computeIfAbsent(parentId, pId -> new Pair<>(
                                                RowSetFactory.builderSequential(), RowSetFactory.builderSequential()));
                                removedAddedPair.first.appendKey(affectedRowKey);
                                removedAddedPair.second.appendKey(affectedRowKey + shiftDelta);
                            });
                        }
                    }
                }
            }
        }
        affectedParents.forEach((parentId, pair) -> {
            try (final RowSet removed = pair.first.build();
                    final RowSet added = pair.second.build()) {
                final WritableRowSet childRows = parentIdToChildRows.get(parentId);
                childRows.remove(removed);
                childRows.insert(added);
            }
        });
    }

    private class Listener extends BaseTable.ListenerImpl {

        private final ModifiedColumnSet inputColumns;

        private Listener() {
            super("tree filter", source, result);
            manage(sourceRowLookup);
            inputColumns = source.newModifiedColumnSet(idColumnName.name(), parentIdColumnName.name());
            Stream.of(filters).flatMap(filter -> Stream.concat(
                    filter.getColumns().stream(),
                    filter.getColumnArrays().stream()))
                    .forEach(inputColumns::setAll);
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            final TableUpdateImpl downstream = new TableUpdateImpl();

            // Our swap listener guarantees that we are on the same step for the source row lookup and source initially,
            // but the sourceRowLookup may not report notifications on every cycle that the source does, if there are
            // no structural changes.
            Assert.leq(sourceRowLookup.getLastNotificationStep(), "sourceRowLookup.getLastNotificationStep()",
                    source.getLastNotificationStep(), "source.getLastNotificationStep()");

            // We can ignore modified while updating if columns we care about were not touched.
            final boolean useModified = upstream.modifiedColumnSet().containsAny(inputColumns);

            // Must take care of removed here, because these rows are not valid in post shift space.
            downstream.removed = resultRows.extract(upstream.removed());

            try (final RowSet allRemoved =
                    useModified ? upstream.removed().union(upstream.getModifiedPreShift()) : null;
                    final RowSet valuesToRemove =
                            (useModified ? allRemoved : upstream.removed()).intersect(matchedSourceRows);
                    final RowSet removedParents =
                            (useModified ? allRemoved : upstream.removed()).intersect(ancestorSourceRows)) {

                removeValues(valuesToRemove);
                ancestorSourceRows.remove(removedParents);
                removeParents(removedParents);
            }

            // Now we must shift all maintained state.
            shiftParentIdToChildRows(upstream.shifted());
            upstream.shifted().apply(matchedSourceRows);
            upstream.shifted().apply(ancestorSourceRows);
            upstream.shifted().apply(resultRows);

            // Finally, handle added sets.
            try (final WritableRowSet addedAndModified = upstream.added().union(upstream.modified());
                    final RowSet newFiltered = filterValues(false, source.getRowSet(), addedAndModified);
                    final RowSet resurrectedParents = checkForResurrectedParent(addedAndModified);
                    final RowSet newParents = computeParents(false, newFiltered);
                    final RowSet newResurrectedParents = computeParents(false, resurrectedParents)) {


                matchedSourceRows.insert(newFiltered);
                ancestorSourceRows.insert(newParents);
                ancestorSourceRows.insert(resurrectedParents);
                ancestorSourceRows.insert(newResurrectedParents);
            }

            // Compute expected results and the sets we will propagate to child listeners.
            try (final RowSet newResultRows = matchedSourceRows.union(ancestorSourceRows);
                    final WritableRowSet resultRemovals = resultRows.minus(newResultRows)) {
                downstream.added = newResultRows.minus(resultRows);
                resultRows.update(downstream.added(), resultRemovals);

                downstream.modified = upstream.modified().intersect(resultRows);
                downstream.modified().writableCast().remove(downstream.added());

                // convert post filter removals into pre-shift space -- note these rows must have previously existed
                upstream.shifted().unapply(resultRemovals);
                downstream.removed().writableCast().insert(resultRemovals);
            }

            downstream.shifted = upstream.shifted();
            downstream.modifiedColumnSet = upstream.modifiedColumnSet(); // note that dependent is a subTable

            result.notifyListeners(downstream);

            validateState(false, source.getRowSet());
        }

        @Override
        public boolean canExecute(final long step) {
            return super.canExecute(step) && sourceRowLookup.satisfied(step);
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

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.api.Selectable;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.impl.hierarchical.BaseHierarchicalTable;
import io.deephaven.engine.table.impl.hierarchical.ReverseLookup;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.table.sort.SortDirective;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.treetable.TreeConstants.RE_TREE_KEY;
import static io.deephaven.treetable.TreeConstants.ROOT_TABLE_KEY;

/**
 * The basic implementation used to produce a viewport-ready snapshot of a tree table, taking into account the set of
 * expanded rows at each level.
 */
public abstract class AbstractTreeSnapshotImpl<INFO_TYPE extends HierarchicalTableInfo> {
    private static final RowSet EMPTY_ROW_SET = RowSetFactory.empty();
    private static final boolean DEBUG =
            Configuration.getInstance().getBooleanWithDefault("AbstractTreeSnapshotImpl.debug", false);

    private final long firstViewportRow;
    private final long lastViewportRow;
    private final BitSet columns;

    private final WhereFilter[] filters;
    private final PreparedSort preparedSort;

    private final Map<Object, TableDetails> tablesByKey;

    private final Set<TreeSnapshotQuery.Operation> includedOps;

    private final TreeTableClientTableManager.Client client;
    private final BaseHierarchicalTable baseTable;
    private final int baseTableId;
    private final INFO_TYPE info;
    private final TreeTableClientTableManager.ClientState clientState;
    private final TreeTableClientTableManager.TreeState treeState;

    private static final Logger log = LoggerFactory.getLogger(AbstractTreeSnapshotImpl.class);

    /**
     * Construct a new query that will create a flat snapshot of the tree table using a flat viewport beginning at the
     * specified rows and columns, applying the specified sorts and filters if required to fetch tables
     * 
     * @param baseTable The ID of the base table. This will be used as a key to maintain this client state.
     * @param tablesByKey The tables within the tree for which viewports are being tracked, separated by table key.
     * @param firstRow The first row of the flat viewport
     * @param lastRow The last row of the flat viewport
     * @param columns The columns to include in the viewport
     * @param filters The filters to applied to new tables.
     * @param sorts The sorts to applied to new tables
     * @param client The CLIENT_TYPE instance
     * @param includedOps The set of operations the client has performed before submitting this TSQ.
     */
    AbstractTreeSnapshotImpl(int baseTableId,
            BaseHierarchicalTable baseTable,
            Map<Object, TableDetails> tablesByKey,
            long firstRow, long lastRow,
            BitSet columns,
            @NotNull WhereFilter[] filters,
            @NotNull List<SortDirective> sorts,
            TreeTableClientTableManager.Client client,
            Set<TreeSnapshotQuery.Operation> includedOps) {
        Assert.leq(firstRow, "firstRow", lastRow, "lastRow");
        Assert.leq(lastRow - firstRow, "lastRow - firstRow", Integer.MAX_VALUE, "Integer.MAX_VALUE");
        this.tablesByKey = tablesByKey;
        this.client = client;
        this.baseTable = baseTable;
        this.baseTableId = baseTableId;
        this.includedOps = includedOps;

        firstViewportRow = firstRow;
        lastViewportRow = lastRow;
        this.columns = columns;
        this.filters = filters;
        this.preparedSort = new PreparedSort(sorts);

        // noinspection unchecked
        this.info = (INFO_TYPE) baseTable.getInfo();

        this.clientState = TreeTableClientTableManager.DEFAULT.get(client);
        this.treeState = clientState.getTreeState(baseTableId, this::makeSnapshotState);
    }

    private SnapshotState makeSnapshotState() {
        return new SnapshotState(baseTable, info.getHierarchicalColumnName());
    }

    /**
     * Apply any required transformations to the root table that are required before the snapshot can proceed. Such
     * transformations should use concurrent instantiation patterns internally.
     * <p>
     * This will be Sort and filter for both {@link RollupSnapshotImpl#prepareRootTable() rollups} and
     * {@link TreeTableSnapshotImpl#prepareRootTable() tree tables}
     * </p>
     *
     * @implNote This method will only be invoked if the client has requested an update using a table id of -1/
     *
     * @return The result of all transformations applied to the root table.
     */
    abstract Table prepareRootTable();

    /**
     * Get if the root table has changed during this TSQ. This can happen if sorts/filters were applied. This will be
     * used to decide if the root needs to be exported to the client.
     *
     * @return true of the root table has changed.
     */
    abstract boolean rootTableChanged();

    /**
     * Get the original source table that was used to create the hierarchical table.
     *
     * @return the original source table
     */
    abstract Table getSourceTable();

    /**
     * Apply any required transformations to child tables that are required before the snapshot can proceed.
     * <p>
     * This will be sorting in the {@link RollupSnapshotImpl#prepareTableInternal(Table) rollup} case, and nothing in
     * the {@link TreeTableSnapshotImpl#prepareTableInternal(Table) tree table} case
     * </p>
     *
     * @implNote This method will only be invoked if the client has requested an update using a table id of -1. This
     *           method will not be invoked on the root table (see {@link #prepareRootTable()}).
     *
     * @param childMap The map in which to lookup children
     * @return The result of all transformations on t
     */
    private Table prepareTable(Object key, TableMap childMap) {
        Table prepared = tryGetRetainedTable(key);

        if (prepared == null) {
            prepared = childMap.get(key);

            if (prepared != null) {
                prepared = prepareTableInternal(prepared);
                retainTable(key, prepared);
            }
        }

        return prepared;
    }

    /**
     * Performs transformations for prepareTable.
     *
     * @param t the table being transformed
     * @return The result of all transformations on t
     */
    abstract Table prepareTableInternal(Table t);

    /**
     * Attach a reverse lookup listener to the specified table.
     */
    Table attachReverseLookup(Table table) {
        final ReverseLookupListener listener = ReverseLookupListener
                .makeReverseLookupListenerWithSnapshot((BaseTable) table, getInfo().getHierarchicalColumnName());
        table = ((BaseTable) table).copy();
        table.setAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, listener);

        return table;
    }

    /**
     * Check if a particular row key is valid with respect to t.
     *
     * @implNote This is used to allow for {@link TreeTableSnapshotImpl} to share {@link ReverseLookup RLLs} across all
     *           child tables.
     *
     * @param usePrev if previous values should be used while validating.
     * @param t The table to validate K with
     * @param key The row key to validate.
     * @return true if key is contained within t
     */
    abstract boolean isKeyValid(boolean usePrev, Table t, long key);

    /**
     * Get the {@link ReverseLookup RLL} associated with t
     *
     * @param t The table to get the RLL for
     * @return The RLL for t
     */
    abstract ReverseLookup getReverseLookup(Table t);

    INFO_TYPE getInfo() {
        return info;
    }

    /**
     * Perform any additional verification needed on a per child basis.
     *
     * @param parentDetail The {@link TableDetails associated with the parent}
     * @param childDetail The {@link TableDetails associated with the parent}
     * @param usePrev if any table data access should use previous values
     * @return true if the child is valid, false if it should be eliminated.
     */
    boolean verifyChild(TableDetails parentDetail, TableDetails childDetail, long childKeyPos, boolean usePrev) {
        return true;
    }

    /**
     * Applies column formats from the HierarchicalTableInfo to a table.
     *
     * @param t the table to format
     * @return the formatted table
     */
    Table applyColumnFormats(Table t) {
        if (info.getColumnFormats() != null && t != null) {
            final Object rll = t.getAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE);
            final Object preparedRll = t.getAttribute(Table.PREPARED_RLL_ATTRIBUTE);
            t = t.updateView(
                    processFormatColumns(t, SelectColumnFactory.getFormatExpressions(getInfo().getColumnFormats())));
            if (rll != null) {
                t.setAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, rll);
            }
            if (preparedRll != null) {
                t.setAttribute(Table.PREPARED_RLL_ATTRIBUTE, preparedRll);
            }
        }

        return t;
    }

    /**
     * Process the initial set of format columns and return a set that is applicable to the table. <br>
     * <br>
     * This is here in case the column type does not match up between levels of a tree, for example, when the table is a
     * rollup and constituent rows are included.
     *
     * @param t the table to update the columns for
     * @param initial the initial set of filter columns
     * @return the applicable set of filter columns for the specified table
     */
    SelectColumn[] processFormatColumns(Table t, SelectColumn[] initial) {
        return initial;
    }

    /**
     * Do any processing required after the snapshot has been computed.
     */
    private void postSnapshot() {
        treeState.releaseIf(k -> {
            final TableDetails details = tablesByKey.get(k);
            return !RE_TREE_KEY.equals(k) && (details == null || details.isRemoved());
        });
    }

    private void checkInputs() {
        final Set<TableDetails> allDetails = new HashSet<>(tablesByKey.values());

        // Make sure that all items that are not ROOT_TABLE_KEY ae the child of something
        // Also make sure every (sweet) child of mine is in the map
        for (final TableDetails detail : tablesByKey.values()) {
            for (final Object child : detail.getChildren()) {
                final TableDetails childDetail = tablesByKey.get(child);
                Assert.neqNull(childDetail, "childDetail");

                allDetails.remove(childDetail);
            }
        }

        if (allDetails.size() != 1) {
            Assert.statementNeverExecuted("There is a detail not corresponding to a requested expansion " + allDetails
                    + " orig = " + tablesByKey);
        }

        Assert.eqTrue(allDetails.contains(tablesByKey.get(ROOT_TABLE_KEY)), "allDetails contains ROOT_TABLE_KEY");
    }

    /**
     * Get a {@link NotificationQueue.Dependency dependency} that will allow us to check whether all "structural"
     * components that may impact a snapshot have been satisfied on the current cycle, if such is needed.
     *
     * @return The appropriate dependency, or null if no such check is needed
     */
    @Nullable
    abstract NotificationQueue.Dependency getRootDependency();

    /**
     * Get a flat snapshot of the table and set of expanded rows.
     *
     * @return A flattened viewport and an updated set of expanded rows.
     */
    public TreeSnapshotResult getSnapshot() {
        if (includedOps.contains(TreeSnapshotQuery.Operation.Close)) {
            clientState.release(baseTableId);
            return new TreeSnapshotResult();
        }

        if (DEBUG) {
            checkInputs();
        }

        preparedSort.computeSortingData();

        final Set<TreeSnapshotQuery.Operation> ops = getOperations();
        // If we're adjusting sorts or filters, we have to wipe out the retention cache.
        if (ops.contains(TreeSnapshotQuery.Operation.SortChanged)
                || ops.contains(TreeSnapshotQuery.Operation.FilterChanged)) {
            releaseAllTables();
        }

        // NB: Our snapshot control must be notification-aware, because if source ticks we cannot guarantee that we
        // won't observe some newly created components on their instantiation step.
        final ConstructSnapshot.SnapshotControl control =
                ConstructSnapshot.makeSnapshotControl(true, baseTable.isRefreshing(),
                        ((NotificationStepSource) baseTable.getSourceTable()));
        final MutableObject<SnapshotState> finalState = new MutableObject<>();
        ConstructSnapshot.callDataSnapshotFunction(getClass().getSimpleName(), control,
                (final boolean usePrev, final long beforeClockValue) -> {
                    try {
                        processTables(usePrev);

                        final TableDetails rootDetails = tablesByKey.get(ROOT_TABLE_KEY);
                        final SnapshotState state = treeState.getUserState();

                        state.beginSnapshot(tablesByKey, columns, firstViewportRow, lastViewportRow);
                        compute(usePrev, rootDetails, state);

                        finalState.setValue(state);
                    } catch (ConstructSnapshot.SnapshotInconsistentException cie) {
                        return false;
                    }
                    return true;
                });
        postSnapshot();

        return makeResult(finalState.getValue());
    }

    /**
     * Do a DFS traversal of the tree to ensure that we have an actual table for each visible table, and the list of
     * children has been updated.
     */
    private void processTables(final boolean usePrev) {
        // Make sure we reset the removed state so if this is a retry we don't have polluted state.
        tablesByKey.values().forEach(td -> td.setRemoved(false));

        final Deque<TableDetails> tablesToProcess = new ArrayDeque<>();

        final TableDetails rootDetail = tablesByKey.get(ROOT_TABLE_KEY);
        final Table fetchedRoot = prepareRootTable();
        maybeWaitForSatisfaction(getRootDependency());
        maybeWaitForSatisfaction(fetchedRoot);

        rootDetail.setTable(fetchedRoot);
        tablesToProcess.push(rootDetail);

        while (!tablesToProcess.isEmpty()) {
            final TableDetails current = tablesToProcess.pop();
            final Set<Object> children = current.getChildren();
            final Table table = current.getTable();
            final ReverseLookup lookup = getReverseLookup(table);

            if (!children.isEmpty()) {
                final TableMap map = getTableMap(table);
                Assert.neqNull(map, "Child table map");

                if (lookup == null) {
                    ConstructSnapshot.failIfConcurrentAttemptInconsistent();
                    Assert.assertion(map.size() == 0, "There should be no child tables", map.size(), "map.size()",
                            children, "children");
                    continue;
                }

                children.forEach(key -> {
                    final TableDetails childDetail = tablesByKey.get(key);
                    final long keyPos = usePrev ? lookup.getPrev(key) : lookup.get(key);

                    // If the row is no longer present for this child key, queue it up to be removed.
                    if (keyPos < 0) {
                        eliminateChildren(childDetail);
                        return;
                    }

                    // The child row existed, now check to see if the child has children.
                    final Table childTable = prepareTable(key, map);
                    maybeWaitForSatisfaction(childTable);
                    if (childTable == null || childTable.isEmpty()) {
                        eliminateChildren(childDetail);
                        return;
                    }

                    childDetail.setTable(childTable);
                    if (!verifyChild(current, childDetail, keyPos, usePrev)) {
                        eliminateChildren(childDetail);
                        return;
                    }

                    tablesToProcess.push(childDetail);
                });
            }
        }
    }

    /**
     * Remove all of the children in the tree below parent from the map of tables that we need to process and inflate.
     *
     * @param details The root child.
     */
    private void eliminateChildren(TableDetails details) {
        final Deque<TableDetails> tablesToEliminate = new ArrayDeque<>();
        tablesToEliminate.push(details);
        while (!tablesToEliminate.isEmpty()) {
            final TableDetails current = tablesToEliminate.pop();
            current.setRemoved(true);
            current.getChildren().stream()
                    .map(tablesByKey::get)
                    .filter(Objects::nonNull)
                    .forEach(tablesToEliminate::add);
        }
    }

    /**
     * @return The TableMap for the specified table
     */
    abstract TableMap getTableMap(Table t);

    /**
     * Create a {@link TreeSnapshotResult} from the result of the viewport calculation.
     *
     * @param state The currenmt SnapshotState object.
     * @return A {@link TreeSnapshotResult} containing the viewport data, and an updated set of {@link TableDetails}.
     */
    private TreeSnapshotResult makeResult(SnapshotState state) {
        final TableDetails[] tables = tablesByKey.values().stream()
                .filter(td -> !td.isRemoved())
                .peek(td -> td.getChildren().removeIf(k -> {
                    final TableDetails child = tablesByKey.get(k);
                    return child == null || child.isRemoved();
                }))
                .toArray(TableDetails[]::new);
        final long actualEnd = firstViewportRow + state.actualViewportSize - 1;
        final Table maybeNewSource = rootTableChanged() ? getSourceTable() : null;
        return new TreeSnapshotResult(maybeNewSource,
                state.totalRowCount,
                state.getDataMatrix(),
                tables,
                state.tableKeyColumn,
                state.childPresenceColumn,
                firstViewportRow,
                actualEnd,
                state.getRequiredConstituents());
    }

    /**
     * Recursively compute and copy the requested viewport. This works by first finding the first visible row, starting
     * with the root table and skipping rows by expansion. Then the tables are walked depth first, by expansion and the
     * data is copied to the resultant flat snapshot, until the proper number of rows have been consumed.
     *
     * @param usePrev Whether we're using previous values or current
     * @param current The current table we're evaluating
     * @param state The current state of the recursion.
     */
    private void compute(final boolean usePrev, final TableDetails current, @NotNull final SnapshotState state) {
        if (current == null) {
            ConstructSnapshot.failIfConcurrentAttemptInconsistent();
            // If this happens that means that the child table has gone away between when we computed the child RowSet,
            // and now.
            // which means the LogicalClock has ticked, and the snapshot is going to fail, so we'll abort mission now.
            Assert.neqNull(current, "Child table ticked away during computation");
        }

        // Where are our expanded rows?
        final Table curTable = current.getTable();
        final TableMap curTableMap = getTableMap(curTable);
        final Set<Object> curChildren = current.getChildren();
        final RowSet expanded = getExpandedIndex(usePrev, curTable, curChildren);
        final RowSet.Iterator exIter = expanded.iterator();

        // Keep track of the position of the first viewported row after taking into account
        // rows skipped.
        long vkUpper;

        final RowSet currentRowSet = usePrev ? curTable.getRowSet().copyPrev() : curTable.getRowSet();

        // If the first row of the viewport is beyond the current table, we'll use an upper that's
        // guaranteed to be beyond the table. One of two things will happen:
        // 1) We have enough expanded rows so that vkUpper gets shifted into this table after
        // child rows are consumed.
        // 2) Even after all the child rows are consumed, it's beyond, in which case we'll
        // skip all of the remaining rows in the table.
        if (firstViewportRow - state.skippedRows >= currentRowSet.size()) {
            vkUpper = Long.MAX_VALUE;
        } else {
            vkUpper = currentRowSet.get(firstViewportRow - state.skippedRows);
        }

        // within this table
        long currentPosition = 0;
        long nextExpansion = -1;

        // When searching for the beginning of the viewport, we need to evaluate all of the expanded rows recursively
        // and
        // shift vkUpper left until there are no more expanded children between the current position and the viewport
        // start.
        final ColumnSource columnSource = curTable.getColumnSource(info.getHierarchicalColumnName());
        while (exIter.hasNext() && (state.skippedRows < firstViewportRow)) {
            final long expandedRow = exIter.nextLong();

            // In this case the beginning of the viewport is before the next expanded row, so we're done. We can
            // start adding these table rows to the viewport for this table.
            if (vkUpper <= expandedRow) {
                nextExpansion = expandedRow;
                break;
            } else {
                // Otherwise, there is an expanded range between the current position and the first viewport row.
                // In this case, we'll recursively evaluate the expanded rows, shifting the initial viewport row to the
                // left
                // by the total number of rows "below" the expanded one.
                final Object tableKey = usePrev ? columnSource.getPrev(expandedRow) : columnSource.get(expandedRow);
                final TableDetails child = tablesByKey.get(tableKey);

                // If the expanded row doesn't exist in the current table RowSet, something at a higher level is broken.
                final long expandedPosition = currentRowSet.find(expandedRow);

                if (expandedPosition < 0) {
                    ConstructSnapshot.failIfConcurrentAttemptInconsistent();
                    Assert.geqZero(expandedPosition, "current.build().find(expandedRow)");
                }

                // Since we know that this expanded row is before the viewport start, we need to accumulate the rows
                // between the current position
                // and this expanded row as "skipped" before moving the current table position to the right.
                state.skippedRows += (expandedPosition - currentPosition) + 1;
                currentPosition = expandedPosition + 1;

                // Next evaluate the expanded child rows of this table.
                compute(usePrev, child, state);

                // If we have skipped the same number of rows as the first viewport row after the recursive evaluation,
                // then
                // we found the first viewport row inside our child and have no more work to do.
                if (state.skippedRows == firstViewportRow) {
                    break;
                }

                if (state.skippedRows >= firstViewportRow) {
                    ConstructSnapshot.failIfConcurrentAttemptInconsistent();
                    Assert.lt(state.skippedRows, "state.skippedRows", firstViewportRow, "firstViewportRow");
                }

                // Finally, we need to shift the row position of the viewport start wrt to this table, by the number of
                // rows we've skipped, less the current position because the current position has already been accounted
                // for in state.skippedRows
                final long newTarget = firstViewportRow - state.skippedRows + currentPosition;
                if (newTarget >= currentRowSet.size()) {
                    vkUpper = Long.MAX_VALUE;
                } else {
                    vkUpper = currentRowSet.get(newTarget);
                }
            }
        }

        // When we get to here, we've found the table and row in that table where the viewport begins, so should start
        // accumulating by table RowSet.

        // There were no more expanded children, so we need to skip the remaining rows in our table, or up to the
        // viewport row whichever comes first.
        if (state.skippedRows < firstViewportRow) {
            final long remaining = firstViewportRow - state.skippedRows;
            if (remaining >= currentRowSet.size() - currentPosition) {
                state.skippedRows += currentRowSet.size() - currentPosition;
                return;
            } else {
                state.skippedRows += remaining;
                currentPosition += remaining;
            }
        }

        if (nextExpansion == -1 && exIter.hasNext()) {
            nextExpansion = exIter.nextLong();
        }

        if (currentPosition >= currentRowSet.size()) {
            return;
        }

        final RowSet.SearchIterator currentIt = currentRowSet.searchIterator();
        if (!currentIt.advance(currentRowSet.get(currentPosition))) {
            return;
        }

        RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        long currentIndexKey = currentIt.currentValue();

        while (state.consumed < state.actualViewportSize) {
            sequentialBuilder.appendKey(currentIndexKey);
            state.consumed++;

            if (nextExpansion == currentIndexKey) {
                // Copy everything so far, and start a new RowSet.
                state.addToSnapshot(usePrev, curTable, current.getKey(), curTableMap, sequentialBuilder.build());
                sequentialBuilder = RowSetFactory.builderSequential();

                final Object tableKey = usePrev ? columnSource.getPrev(nextExpansion) : columnSource.get(nextExpansion);
                final TableDetails child = tablesByKey.get(tableKey);

                if (child == null) {
                    ConstructSnapshot.failIfConcurrentAttemptInconsistent();
                    log.error().append("No details for key ").append(Objects.toString(tableKey)).append(", usePrev=")
                            .append(usePrev).append(", nextExpansion=").append(nextExpansion).endl();
                    Assert.statementNeverExecuted();
                }

                compute(usePrev, child, state);

                if (exIter.hasNext()) {
                    nextExpansion = exIter.nextLong();
                }
            }

            if (!currentIt.hasNext()) {
                break;
            }
            currentIndexKey = currentIt.nextLong();
        }

        final RowSet remainingToCopy = sequentialBuilder.build();
        if (!remainingToCopy.isEmpty()) {
            state.addToSnapshot(usePrev, curTable, current.getKey(), curTableMap, remainingToCopy);
        }
    }

    /**
     * Use the {@link ReverseLookup} provided by the specific implementation to locate where client-expanded rows have
     * moved within the table, and return a RowSet of these rows.
     *
     * @param usePrev If we should use previous values
     * @param t The table to look in
     * @param childKeys The keys of the child tables to find
     * @return A RowSet containing the rows that represent the indices of the tables indicated in childKeys, if they
     *         still exist.
     */
    private RowSet getExpandedIndex(boolean usePrev, Table t, Set<Object> childKeys) {
        final ReverseLookup lookup = getReverseLookup(t);

        if (lookup == null) {
            return EMPTY_ROW_SET;
        }

        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        childKeys.stream().filter(k -> {
            final TableDetails td = tablesByKey.get(k);
            return td != null && !td.isRemoved();
        }).forEach(id -> {
            final long key = usePrev ? lookup.getPrev(id) : lookup.get(id);
            if (key >= 0 && isKeyValid(usePrev, t, key)) {
                builder.addKey(key);
            }
        });

        return builder.build();
    }

    Table applySorts(@NotNull final Table table) {
        return preparedSort.applySorts(table);
    }

    @VisibleForTesting
    static class PreparedSort {
        private final List<SortDirective> directives;

        private List<String> absColumns;
        private List<String> absViews;
        private List<Pair<String, Integer>> colDirectivePairs;

        PreparedSort(@NotNull final List<SortDirective> directives) {
            this.directives = directives;
        }

        /**
         * Take a preemptive pass through the sorting parameters and compute any additional columns required to perform
         * the sort once, so we can apply them at each level without creating lots of extra garbage.
         */
        void computeSortingData() {
            if (directives.isEmpty()) {
                absColumns = absViews = Collections.emptyList();
                colDirectivePairs = Collections.emptyList();
                return;
            }

            absColumns = new ArrayList<>();
            absViews = new ArrayList<>();
            colDirectivePairs = new ArrayList<>();

            for (final SortDirective directive : directives) {
                final int sortType = directive.getDirection();
                String sortColumn = directive.getColumnName();
                if (sortType != SortDirective.NOT_SORTED && directive.isAbsolute()) {
                    final String absName = "__ABS__" + sortColumn;
                    absColumns.add(absName);
                    absViews.add(absName + " = abs(" + sortColumn + ")");

                    sortColumn = absName;
                }

                colDirectivePairs.add(new Pair<>(sortColumn, directive.getDirection()));
            }
        }

        /**
         * Apply requested sorts and filters to the specified table, if needed. This method makes no assumptions about,
         * or changes to {@link ReverseLookup ReverseLookups}
         *
         * @param table The table to sort and filter.
         * @return The result of applying all of the requested sorts.
         */
        Table applySorts(@NotNull Table table) {
            if (directives.isEmpty()) {
                return table;
            }

            Table modified = table;

            if (!absViews.isEmpty()) {
                modified = modified.updateView(Selectable.from(absViews));
            }

            for (int i = colDirectivePairs.size() - 1; i >= 0; i--) {
                final Pair<String, Integer> sort = colDirectivePairs.get(i);
                switch (sort.second) {
                    case SortDirective.ASCENDING:
                        modified = modified.sort(sort.first);
                        break;
                    case SortDirective.DESCENDING:
                        modified = modified.sortDescending(sort.first);
                        break;
                }
            }

            if (!absColumns.isEmpty()) {
                modified = modified.dropColumns(absColumns);
            }

            return modified;
        }
    }

    /**
     * @return The set of {@link WhereFilter filters} to be applied to the table.
     */
    WhereFilter[] getFilters() {
        return filters;
    }

    /**
     * @return The list of {@link SortDirective sorts} to be applied to the table.
     */
    List<SortDirective> getDirectives() {
        return preparedSort.directives;
    }

    BaseHierarchicalTable getBaseTable() {
        return baseTable;
    }

    TreeTableClientTableManager.Client getClient() {
        return client;
    }

    protected Set<TreeSnapshotQuery.Operation> getOperations() {
        return includedOps;
    }

    final Table tryGetRetainedTable(Object key) {
        return treeState.getTable(key);
    }

    final void retainTable(Object key, Table table) {
        treeState.retain(key, table);
    }

    final void releaseAllTables() {
        treeState.releaseAll();
    }

    private static void maybeWaitForSatisfaction(@Nullable final Table table) {
        maybeWaitForSatisfaction((NotificationQueue.Dependency) table);
    }

    private static void maybeWaitForSatisfaction(@Nullable final NotificationQueue.Dependency dependency) {
        ConstructSnapshot.maybeWaitForSatisfaction(dependency);
    }
}

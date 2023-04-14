/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTable.Proxy;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * {@link PartitionedTable.Proxy} implementation.
 */
class PartitionedTableProxyImpl extends LivenessArtifact implements PartitionedTable.Proxy {

    /**
     * Make a {@link PartitionedTable.Proxy proxy} to the supplied {@code target}.
     *
     * @param target The target {@link PartitionedTable} whose constituents should be used when proxying
     *        {@link TableOperations table operations}
     * @param requireMatchingKeys As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @param sanityCheckJoins As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @return A {@link PartitionedTable.Proxy proxy} to {@code target}
     */
    static PartitionedTable.Proxy of(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
        return new PartitionedTableProxyImpl(target, requireMatchingKeys, sanityCheckJoins);
    }

    private static final ColumnName FOUND_IN = ColumnName.of("__FOUND_IN__");
    private static final ColumnName ENCLOSING_CONSTITUENT = ColumnName.of("__ENCLOSING_CONSTITUENT__");

    /**
     * The underlying target {@link PartitionedTable}.
     */
    private final PartitionedTable target;

    /**
     * Whether to require that partitioned table arguments used in multi-table operations have matching key sets.
     */
    private final boolean requireMatchingKeys;

    /**
     * Whether to check that join operations don't have overlapping keys in any constituents of their partitioned table
     * inputs.
     */
    private final boolean sanityCheckJoins;

    private PartitionedTableProxyImpl(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
        if (target.table().isRefreshing()) {
            manage(target);
        }
        this.target = target;
        this.requireMatchingKeys = requireMatchingKeys;
        this.sanityCheckJoins = sanityCheckJoins;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final PartitionedTableProxyImpl that = (PartitionedTableProxyImpl) other;
        return requireMatchingKeys == that.requireMatchingKeys
                && sanityCheckJoins == that.sanityCheckJoins
                && target.equals(that.target);
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + target.hashCode();
        result = 31 * result + Boolean.hashCode(requireMatchingKeys);
        result = 31 * result + Boolean.hashCode(sanityCheckJoins);
        return result;
    }

    @Override
    public String toString() {
        return "PartitionedTable.Proxy for " + target.table().getDescription();
    }

    @Override
    public PartitionedTable target() {
        return target;
    }

    @Override
    public boolean requiresMatchingKeys() {
        return requireMatchingKeys;
    }

    @Override
    public boolean sanityChecksJoins() {
        return sanityCheckJoins;
    }

    private static ExecutionContext getOrCreateExecutionContext(final boolean requiresFullContext) {
        ExecutionContext context = ExecutionContext.getContextToRecord();
        if (context == null) {
            final ExecutionContext.Builder builder = ExecutionContext.newBuilder()
                    .captureQueryCompiler()
                    .markSystemic();
            if (requiresFullContext) {
                builder.newQueryLibrary();
                builder.emptyQueryScope();
            }
            context = builder.build();
        }
        return context;
    }

    private PartitionedTable.Proxy basicTransform(@NotNull final UnaryOperator<Table> transformer) {
        return basicTransform(false, transformer);
    }

    private PartitionedTable.Proxy basicTransform(
            final boolean requiresFullContext, @NotNull final UnaryOperator<Table> transformer) {
        return new PartitionedTableProxyImpl(
                target.transform(
                        getOrCreateExecutionContext(requiresFullContext),
                        transformer,
                        target.table().isRefreshing()),
                requireMatchingKeys,
                sanityCheckJoins);
    }

    private PartitionedTable.Proxy complexTransform(
            @NotNull final TableOperations<?, ?> other,
            @NotNull final BinaryOperator<Table> transformer,
            @Nullable final Collection<? extends JoinMatch> joinMatches) {
        return complexTransform(false, other, transformer, joinMatches);
    }

    private PartitionedTable.Proxy complexTransform(
            final boolean requiresFullContext,
            @NotNull final TableOperations<?, ?> other,
            @NotNull final BinaryOperator<Table> transformer,
            @Nullable final Collection<? extends JoinMatch> joinMatches) {
        final ExecutionContext context = getOrCreateExecutionContext(requiresFullContext);
        if (other instanceof Table) {
            final Table otherTable = (Table) other;
            final boolean refreshingResults = target.table().isRefreshing() || otherTable.isRefreshing();
            if (refreshingResults && joinMatches != null) {
                UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
            }
            return new PartitionedTableProxyImpl(
                    target.transform(context, ct -> transformer.apply(ct, otherTable), refreshingResults),
                    requireMatchingKeys,
                    sanityCheckJoins);
        }
        if (other instanceof PartitionedTable.Proxy) {
            final PartitionedTable.Proxy otherProxy = (PartitionedTable.Proxy) other;
            final PartitionedTable otherTarget = otherProxy.target();
            final boolean refreshingResults = target.table().isRefreshing() || otherTarget.table().isRefreshing();

            if (target.table().isRefreshing() || otherTarget.table().isRefreshing()) {
                UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
            }

            final MatchPair[] keyColumnNamePairs = PartitionedTableImpl.matchKeyColumns(target, otherTarget);
            final DependentValidation uniqueKeys = requireMatchingKeys
                    ? matchingKeysValidation(target, otherTarget, keyColumnNamePairs)
                    : null;
            final DependentValidation overlappingLhsJoinKeys = sanityCheckJoins && joinMatches != null
                    ? overlappingLhsJoinKeysValidation(target, joinMatches)
                    : null;
            final DependentValidation overlappingRhsJoinKeys = sanityCheckJoins && joinMatches != null
                    ? overlappingRhsJoinKeysValidation(otherTarget, joinMatches)
                    : null;

            final Table validatedLhsTable = validated(target.table(), uniqueKeys, overlappingLhsJoinKeys);
            final Table validatedRhsTable = validated(otherTarget.table(), uniqueKeys, overlappingRhsJoinKeys);
            final PartitionedTable lhsToUse = maybeRewrap(validatedLhsTable, target);
            final PartitionedTable rhsToUse = maybeRewrap(validatedRhsTable, otherTarget);

            return new PartitionedTableProxyImpl(
                    lhsToUse.partitionedTransform(rhsToUse, context, transformer, refreshingResults),
                    requireMatchingKeys,
                    sanityCheckJoins);
        }
        throw new IllegalArgumentException("Unexpected TableOperations input " + other
                + ", expected Table or PartitionedTable.Proxy");
    }

    /**
     * Struct to pair a {@code validation} with a {@code table} whose updates dictate when the validation needs to run.
     */
    private static class DependentValidation {

        private final String name;
        private final Table table;
        private final Runnable validation;

        private DependentValidation(
                @NotNull final String name,
                @NotNull final Table table,
                @NotNull final Runnable validation) {
            this.name = name;
            this.table = table;
            this.validation = validation;
        }
    }

    private static Table validated(
            @NotNull final Table parent,
            @NotNull final DependentValidation... dependentValidationsIn) {
        if (dependentValidationsIn.length == 0 || !parent.isRefreshing()) {
            return parent;
        }
        final DependentValidation[] dependentValidations =
                Arrays.stream(dependentValidationsIn).filter(Objects::nonNull).toArray(DependentValidation[]::new);
        if (dependentValidations.length == 0) {
            return parent;
        }

        // NB: All code paths that pass non-null validations for refreshing parents call checkInitiateTableOperation
        // first, so we can dispense with snapshots and swap listeners.
        final QueryTable coalescedParent = (QueryTable) parent.coalesce();
        final QueryTable child = coalescedParent.getSubTable(
                coalescedParent.getRowSet(),
                coalescedParent.getModifiedColumnSetForUpdates(),
                coalescedParent.getAttributes());
        coalescedParent.propagateFlatness(child);

        final List<ListenerRecorder> recorders = new ArrayList<>(1 + dependentValidations.length);

        final ListenerRecorder parentRecorder = new ListenerRecorder("Validating Copy Parent", coalescedParent, null);
        coalescedParent.addUpdateListener(parentRecorder);
        recorders.add(parentRecorder);

        final ListenerRecorder[] validationRecorders = Arrays.stream(dependentValidations).map(dv -> {
            final ListenerRecorder validationRecorder = new ListenerRecorder(dv.name, dv.table, null);
            dv.table.addUpdateListener(validationRecorder);
            recorders.add(validationRecorder);
            return validationRecorder;
        }).toArray(ListenerRecorder[]::new);

        final MergedListener validationMergeListener = new MergedListener(recorders, List.of(), "Validation", child) {
            @Override
            protected void process() {
                final int numValidations = dependentValidations.length;
                for (int vi = 0; vi < numValidations; ++vi) {
                    if (validationRecorders[vi].recordedVariablesAreValid()) {
                        dependentValidations[vi].validation.run();
                    }
                }
                final TableUpdate parentUpdate = parentRecorder.getUpdate();
                if (parentUpdate != null && !parentUpdate.empty()) {
                    parentUpdate.acquire();
                    child.notifyListeners(parentUpdate);
                }
            }
        };

        recorders.forEach(rec -> rec.setMergedListener(validationMergeListener));
        child.addParentReference(validationMergeListener);
        return child;
    }

    /**
     * Make and run a dependent validation checking for keys that are uniquely in only {@code lhs} or {@code rhs}.
     *
     * @param lhs The left-hand-side (first) partitioned table
     * @param rhs The right-hand-side (second) partitioned table
     * @param keyColumnNamePairs Pairs linking key column names in {@code lhs} and {@code rhs}
     * @return A dependent validation checking for keys that are uniquely in only one of the input partitioned tables
     */
    private static DependentValidation matchingKeysValidation(
            @NotNull final PartitionedTable lhs,
            @NotNull final PartitionedTable rhs,
            @NotNull final MatchPair[] keyColumnNamePairs) {
        final String[] lhsKeyColumnNames = Arrays.stream(keyColumnNamePairs)
                .map(MatchPair::leftColumn).toArray(String[]::new);
        final SourceColumn[] rhsKeyColumnRenames = Arrays.stream(keyColumnNamePairs)
                .map(mp -> new SourceColumn(mp.rightColumn(), mp.leftColumn())).toArray(SourceColumn[]::new);
        final Table lhsKeys = lhs.table().selectDistinct(lhsKeyColumnNames);
        final Table rhsKeys = rhs.table().updateView(rhsKeyColumnRenames).selectDistinct(lhsKeyColumnNames);
        final Table unionedKeys = TableTools.merge(lhsKeys, rhsKeys);
        final Table countedKeys = unionedKeys.countBy(FOUND_IN.name(), lhs.keyColumnNames());
        final Table nonMatchingKeys = countedKeys.where(new MatchFilter(FOUND_IN.name(), 1));
        final Table nonMatchingKeysOnly = nonMatchingKeys.view(lhsKeyColumnNames);
        checkNonMatchingKeys(nonMatchingKeysOnly);
        return new DependentValidation("Matching Partition Keys", nonMatchingKeysOnly,
                () -> checkNonMatchingKeys(nonMatchingKeysOnly));
    }

    private static void checkNonMatchingKeys(@NotNull final Table nonMatchingKeys) {
        if (!nonMatchingKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Partitioned table arguments have non-matching keys; re-assess your input data or create a proxy with requireMatchingKeys=false:\n"
                            + TableTools.string(nonMatchingKeys, 10));
        }
    }

    private static DependentValidation overlappingLhsJoinKeysValidation(
            @NotNull final PartitionedTable lhs,
            @NotNull final Collection<? extends JoinMatch> joinMatches) {
        return nonOverlappingJoinKeysValidation(lhs,
                joinMatches.stream().map(jm -> jm.left().name()).toArray(String[]::new));
    }

    private static DependentValidation overlappingRhsJoinKeysValidation(
            @NotNull final PartitionedTable rhs,
            @NotNull final Collection<? extends JoinMatch> joinMatches) {
        return nonOverlappingJoinKeysValidation(rhs,
                joinMatches.stream().map(jm -> jm.right().name()).toArray(String[]::new));
    }

    /**
     * Make and run a dependent validation checking join keys that are found in more than one constituent table in
     * {@code input}.
     *
     * @param input The input partitioned table
     * @param joinKeyColumnNames The exact match key column names for the join operation
     * @return A dependent validation checking for join keys that are found in more than one constituent table in
     *         {@code input}
     */
    private static DependentValidation nonOverlappingJoinKeysValidation(
            @NotNull final PartitionedTable input,
            @NotNull final String[] joinKeyColumnNames) {
        // NB: At the moment, we are assuming that constituents appear only once per partitioned table in scenarios
        // where overlapping join keys are concerning.
        final AtomicLong sequenceCounter = new AtomicLong(0);
        final PartitionedTable stamped = input.transform(
                null,
                table -> table.updateView(
                        new LongConstantColumn(ENCLOSING_CONSTITUENT.name(), sequenceCounter.getAndIncrement())),
                input.table().isRefreshing());
        final Table merged = stamped.merge();
        final Table mergedWithUniqueAgg = merged.aggAllBy(AggSpec.unique(), joinKeyColumnNames);
        final Table overlappingJoinKeys = mergedWithUniqueAgg.where(Filter.isNull(ENCLOSING_CONSTITUENT));
        final Table overlappingJoinKeysOnly = overlappingJoinKeys.view(joinKeyColumnNames);
        checkOverlappingJoinKeys(input, overlappingJoinKeysOnly);
        return new DependentValidation("Non-overlapping Join Keys", overlappingJoinKeysOnly,
                () -> checkOverlappingJoinKeys(input, overlappingJoinKeysOnly));
    }

    private static void checkOverlappingJoinKeys(
            @NotNull final PartitionedTable input,
            @NotNull final Table overlappingJoinKeys) {
        if (!overlappingJoinKeys.isEmpty()) {
            throw new IllegalArgumentException("Partitioned table \"" + input.table().getDescription()
                    + "\" has join keys found in multiple constituents; re-assess your input data or create a proxy with sanityCheckJoinOperations=false:\n"
                    + TableTools.string(overlappingJoinKeys, 10));
        }
    }

    private static PartitionedTable maybeRewrap(@NotNull final Table table, @NotNull final PartitionedTable existing) {
        return table == existing.table()
                ? existing
                : new PartitionedTableImpl(table, existing.keyColumnNames(), existing.uniqueKeys(),
                        existing.constituentColumnName(), existing.constituentDefinition(),
                        existing.constituentChangesPermitted(), false);
    }

    // region TableOperations Implementation

    @Override
    public PartitionedTable.Proxy head(long size) {
        return basicTransform(ct -> ct.head(size));
    }

    @Override
    public PartitionedTable.Proxy tail(long size) {
        return basicTransform(ct -> ct.tail(size));
    }

    @Override
    public PartitionedTable.Proxy reverse() {
        return basicTransform(Table::reverse);
    }

    @Override
    public PartitionedTable.Proxy snapshot() {
        return basicTransform(TableOperations::snapshot);
    }

    @Override
    public PartitionedTable.Proxy snapshotWhen(TableOperations<?, ?> trigger, Flag... features) {
        return complexTransform(trigger, (base, tr) -> base.snapshotWhen(tr, features), null);
    }

    @Override
    public PartitionedTable.Proxy snapshotWhen(TableOperations<?, ?> trigger, Collection<Flag> features,
            String... stampColumns) {
        return complexTransform(trigger, (base, tr) -> base.snapshotWhen(tr, features, stampColumns), null);
    }

    @Override
    public PartitionedTable.Proxy snapshotWhen(TableOperations<?, ?> trigger, SnapshotWhenOptions options) {
        return complexTransform(trigger, (base, tr) -> base.snapshotWhen(tr, options), null);
    }

    @Override
    public PartitionedTable.Proxy sort(Collection<SortColumn> columnsToSortBy) {
        return basicTransform(ct -> ct.sort(columnsToSortBy));
    }

    @Override
    public PartitionedTable.Proxy where(Collection<? extends Filter> filters) {
        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        final TableDefinition definition = target.constituentDefinition();
        for (WhereFilter filter : whereFilters) {
            filter.init(definition);
        }
        return basicTransform(ct -> ct.where(WhereFilter.copyFrom(whereFilters)));
    }

    @Override
    public PartitionedTable.Proxy whereIn(TableOperations<?, ?> rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereIn(ot, columnsToMatch), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy whereNotIn(TableOperations<?, ?> rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return complexTransform(rightTable, (ct, ot) -> ct.whereNotIn(ot, columnsToMatch), columnsToMatch);
    }

    @NotNull
    private SelectColumn[] toSelectColumns(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = SelectColumn.from(columns);
        SelectAndViewAnalyzer.initializeSelectColumns(
                target.constituentDefinition().getColumnNameMap(), selectColumns);
        return selectColumns;
    }

    @Override
    public PartitionedTable.Proxy view(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.view(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy updateView(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.updateView(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy update(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.update(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy lazyUpdate(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.lazyUpdate(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy select(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.select(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy naturalJoin(TableOperations<?, ?> rightTable,
            Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.naturalJoin(ot, columnsToMatch, columnsToAdd),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy exactJoin(TableOperations<?, ?> rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return complexTransform(rightTable, (ct, ot) -> ct.exactJoin(ot, columnsToMatch, columnsToAdd), columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations<?, ?> rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return join(rightTable, columnsToMatch, columnsToAdd, CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    @Override
    public PartitionedTable.Proxy join(TableOperations<?, ?> rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return complexTransform(rightTable, (ct, ot) -> ct.join(ot, columnsToMatch, columnsToAdd, reserveBits),
                columnsToMatch);
    }

    @Override
    public PartitionedTable.Proxy aj(TableOperations<?, ?> rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return complexTransform(rightTable, (ct, ot) -> ct.aj(ot, columnsToMatch, columnsToAdd, asOfJoinRule),
                columnsToMatch.stream().limit(columnsToMatch.size() - 1).collect(Collectors.toList()));
    }

    @Override
    public PartitionedTable.Proxy raj(TableOperations<?, ?> rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return complexTransform(rightTable, (ct, ot) -> ct.raj(ot, columnsToMatch, columnsToAdd, reverseAsOfJoinRule),
                columnsToMatch.stream().limit(columnsToMatch.size() - 1).collect(Collectors.toList()));
    }

    @Override
    public Proxy rangeJoin(TableOperations<?, ?> rightTable, Collection<? extends JoinMatch> exactMatches,
            RangeJoinMatch rangeMatch, Collection<? extends Aggregation> aggregations) {
        return complexTransform(rightTable, (ct, ot) -> ct.rangeJoin(ot, exactMatches, rangeMatch, aggregations),
                exactMatches);
    }

    @Override
    public PartitionedTable.Proxy aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return basicTransform(true, ct -> ct.aggAllBy(spec, groupByColumns));
    }

    @Override
    public PartitionedTable.Proxy aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            TableOperations<?, ?> initialGroups, Collection<? extends ColumnName> groupByColumns) {
        if (initialGroups == null) {
            return basicTransform(true, ct -> ct.aggBy(aggregations, preserveEmpty, null, groupByColumns));
        }
        return complexTransform(true, initialGroups,
                (ct, ot) -> ct.aggBy(aggregations, preserveEmpty, ot, groupByColumns),
                null);
    }

    @Override
    public PartitionedTable.Proxy updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return basicTransform(ct -> ct.updateBy(control, operations, byColumns));
    }

    @Override
    public PartitionedTable.Proxy selectDistinct() {
        return basicTransform(Table::selectDistinct);
    }

    @Override
    public PartitionedTable.Proxy selectDistinct(Collection<? extends Selectable> columns) {
        final SelectColumn[] selectColumns = toSelectColumns(columns);
        return basicTransform(ct -> ct.selectDistinct(SelectColumn.copyFrom(selectColumns)));
    }

    @Override
    public PartitionedTable.Proxy ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return basicTransform(ct -> ct.ungroup(nullFill, columnsToUngroup));
    }

    @Override
    public PartitionedTable.Proxy dropColumns(String... columnNames) {
        return basicTransform(ct -> ct.dropColumns(columnNames));
    }

    // endregion TableOperations Implementation
}

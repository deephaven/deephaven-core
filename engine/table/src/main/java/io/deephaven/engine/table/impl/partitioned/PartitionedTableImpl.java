package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link PartitionedTable} implementation.
 */
public class PartitionedTableImpl extends LivenessArtifact implements PartitionedTable {

    private static final ColumnName RHS_CONSTITUENT = ColumnName.of("__RHS_CONSTITUENT__");

    private final Table table;
    private final Set<String> keyColumnNames;
    private final String constituentColumnName;
    private final TableDefinition constituentDefinition;

    /**
     * @see io.deephaven.engine.table.PartitionedTableFactory#of(Table, Set, String, TableDefinition) Factory method
     *      that delegates to this method
     */
    PartitionedTableImpl(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition) {
        if (table.isRefreshing()) {
            manage(table);
        }
        this.table = table;
        this.keyColumnNames = Collections.unmodifiableSet(new LinkedHashSet<>(keyColumnNames));
        this.constituentColumnName = constituentColumnName;
        this.constituentDefinition = constituentDefinition;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final PartitionedTableImpl that = (PartitionedTableImpl) other;
        return table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public String toString() {
        return "PartitionedTable for " + table.getDescription();
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public Set<String> keyColumnNames() {
        return keyColumnNames;
    }

    @Override
    public String constituentColumnName() {
        return constituentColumnName;
    }

    @Override
    public TableDefinition constituentDefinition() {
        return constituentDefinition;
    }

    @Override
    public PartitionedTable.Proxy proxy(final boolean requireMatchingKeys, final boolean sanityCheckJoinOperations) {
        return PartitionedTableProxyImpl.of(this, requireMatchingKeys, sanityCheckJoinOperations);
    }

    @Override
    public Table merge() {
        if (!table.isRefreshing()) {
            final Iterator<Table> constituents = table.objectColumnIterator(constituentColumnName);
            return TableTools.merge(StreamSupport.stream(Spliterators.spliterator(
                            constituents, table.size(), Spliterator.ORDERED), false)
                    .toArray(Table[]::new));
        }
        throw new UnsupportedOperationException("TODO-RWC");
    }

    @Override
    public PartitionedTable filter(@NotNull final Collection<? extends Filter> filters) {
        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        final boolean invalidFilter = Arrays.stream(whereFilters).flatMap((final WhereFilter filter) -> {
            filter.init(table.getDefinition());
            return Stream.concat(filter.getColumns().stream(), filter.getColumnArrays().stream());
        }).anyMatch((final String columnName) -> columnName.equals(constituentColumnName));
        if (invalidFilter) {
            throw new IllegalArgumentException("Unsupported filter against constituent column " + constituentColumnName
                    + " found in filters: " + filters);
        }
        return new PartitionedTableImpl(
                table.where(whereFilters),
                keyColumnNames,
                constituentColumnName,
                constituentDefinition);
    }

    @Override
    public PartitionedTable transform(@NotNull final Function<Table, Table> transformer) {
        final LivenessScope operationScope = new LivenessScope();
        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        try (final SafeCloseable ignored = LivenessScopeStack.open(operationScope, false)) {
            // Perform the transformation
            resultTable = table.update(new TableTransformationColumn(constituentColumnName, transformer));

            // Make sure we have a valid result constituent definition
            final Table emptyConstituent = emptyConstituent(constituentDefinition);
            final Table resultEmptyConstituent = transformer.apply(emptyConstituent);
            resultConstituentDefinition = resultEmptyConstituent.getDefinition();

            // Ensure that we don't unnecessarily keep the empty constituents around
            operationScope.unmanage(emptyConstituent);
            operationScope.unmanage(resultEmptyConstituent);
        }
        operationScope.transferTo(LivenessScopeStack.peek());

        // Build the result partitioned table
        return new PartitionedTableImpl(
                resultTable,
                keyColumnNames,
                constituentColumnName,
                resultConstituentDefinition);
    }

    @Override
    public PartitionedTable partitionedTransform(
            @NotNull final PartitionedTable other,
            @NotNull final BinaryOperator<Table> transformer) {
        // Check safety before doing any extra work
        if (table.isRefreshing() || other.table().isRefreshing()) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }

        // Validate join compatibility
        checkMatchingKeyColumns(this, other);

        final LivenessScope operationScope = new LivenessScope();
        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        try (final SafeCloseable ignored = LivenessScopeStack.open(operationScope, false)) {
            // Perform the transformation
            final Table joined = table.join(
                    other.table(),
                    JoinMatch.from(keyColumnNames),
                    List.of(JoinAddition.of(RHS_CONSTITUENT,
                            ColumnName.of(other.constituentColumnName()))));
            resultTable = joined.update(new BiTableTransformationColumn(
                    constituentColumnName, RHS_CONSTITUENT.name(), transformer));

            // Make sure we have a valid result constituent definition
            final Table emptyConstituent1 = emptyConstituent(constituentDefinition);
            final Table emptyConstituent2 = emptyConstituent(other.constituentDefinition());
            final Table resultEmptyConstituent = transformer.apply(emptyConstituent1, emptyConstituent2);
            resultConstituentDefinition = resultEmptyConstituent.getDefinition();

            // Ensure that we don't unnecessarily keep the empty constituents around
            operationScope.unmanage(emptyConstituent1);
            operationScope.unmanage(emptyConstituent2);
            operationScope.unmanage(resultEmptyConstituent);
        }
        operationScope.transferTo(LivenessScopeStack.peek());

        // Build the result partitioned table
        return new PartitionedTableImpl(
                resultTable,
                keyColumnNames,
                constituentColumnName,
                resultConstituentDefinition);
    }

    /**
     * Validate that {@code lhs} and {@code rhs} have compatible (same name, same type) key columns, allowing
     * {@link #partitionedTransform(PartitionedTable, BinaryOperator)}.
     *
     * @param lhs The first partitioned table
     * @param rhs The second partitioned table
     * @throws IllegalArgumentException If the key columns are mismatched
     */
    static void checkMatchingKeyColumns(@NotNull final PartitionedTable lhs, @NotNull final PartitionedTable rhs) {
        final Map<String, ColumnDefinition<?>> lhsKeyColumnDefinitions = keyColumnDefinitions(lhs);
        final Map<String, ColumnDefinition<?>> rhsKeyColumnDefinitions = keyColumnDefinitions(rhs);
        if (!lhs.keyColumnNames().equals(rhs.keyColumnNames())) {
            throw new IllegalArgumentException("Incompatible partitioned table input for partitioned transform; "
                    + "key column sets don't contain the same names: "
                    + "first has " + lhsKeyColumnDefinitions.values()
                    + ", second has " + rhsKeyColumnDefinitions.values());
        }
        final String typeMismatches = lhsKeyColumnDefinitions.values().stream()
                .filter(cd -> !cd.isCompatible(rhsKeyColumnDefinitions.get(cd.getName())))
                .map(cd -> cd.describeForCompatibility() + " doesn't match "
                        + rhsKeyColumnDefinitions.get(cd.getName()).describeForCompatibility())
                .collect(Collectors.joining(", "));
        if (!typeMismatches.isEmpty()) {
            throw new IllegalArgumentException("Incompatible partitioned table input for partitioned transform; "
                    + "key column definitions don't match: " + typeMismatches);
        }
    }

    private static Table emptyConstituent(@NotNull final TableDefinition constituentDefinition) {
        // noinspection resource
        return new QueryTable(
                constituentDefinition,
                RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(constituentDefinition));
    }

    private static Map<String, ColumnDefinition<?>> keyColumnDefinitions(
            @NotNull final PartitionedTable partitionedTable) {
        final Set<String> keyColumnNames = partitionedTable.keyColumnNames();
        return partitionedTable.table().getDefinition().getColumnStream()
                .filter(cd -> keyColumnNames.contains(cd.getName()))
                .collect(Collectors.toMap(ColumnDefinition::getName, Function.identity()));
    }

    // TODO-RWC: Add ticket for this
    // TODO (insert ticket here): Support "PartitionedTable withCombinedKeys(String keyColumnName)" for combining
    // multiple key columns into a compound key column using the tuple library, and then add "transformWithKeys"
    // support.

}

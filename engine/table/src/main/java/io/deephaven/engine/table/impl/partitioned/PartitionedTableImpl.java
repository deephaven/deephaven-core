package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.engine.table.iterators.ColumnIterator.*;

/**
 * {@link PartitionedTable} implementation.
 */
public class PartitionedTableImpl extends LivenessArtifact implements PartitionedTable {

    private static final ColumnName RHS_CONSTITUENT = ColumnName.of("__RHS_CONSTITUENT__");

    private final Table table;
    private final Set<String> keyColumnNames;
    private final String constituentColumnName;
    private final TableDefinition constituentDefinition;
    private final boolean constituentChangesPermitted;

    /**
     * @see io.deephaven.engine.table.PartitionedTableFactory#of(Table, Set, String, TableDefinition, boolean) Factory
     *      method that delegates to this method
     * @apiNote Only engine-internal tools should call this constructor directly
     */
    public PartitionedTableImpl(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition,
            final boolean constituentChangesPermitted,
            final boolean validateConstituents) {
        if (validateConstituents) {
            final QueryTable coalesced = (QueryTable) table.coalesce();
            this.table = coalesced.getResult(
                    new ValidateConstituents(coalesced, constituentColumnName, constituentDefinition));
        } else {
            this.table = table;
        }
        if (this.table.isRefreshing()) {
            manage(this.table);
        }
        this.keyColumnNames = Collections.unmodifiableSet(new LinkedHashSet<>(keyColumnNames));
        this.constituentColumnName = constituentColumnName;
        this.constituentDefinition = constituentDefinition;
        this.constituentChangesPermitted = constituentChangesPermitted;
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
    public boolean constituentChangesPermitted() {
        return constituentChangesPermitted;
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
                constituentDefinition,
                constituentChangesPermitted || table.isRefreshing(),
                false);
    }

    @Override
    public PartitionedTable transform(@NotNull final Function<Table, Table> transformer) {
        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        final LivenessManager enclosingScope = LivenessScopeStack.peek();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            // Perform the transformation
            resultTable = table.update(new TableTransformationColumn(constituentColumnName, transformer));
            enclosingScope.manage(resultTable);

            // Make sure we have a valid result constituent definition
            final Table emptyConstituent = emptyConstituent(constituentDefinition);
            final Table resultEmptyConstituent = transformer.apply(emptyConstituent);
            resultConstituentDefinition = resultEmptyConstituent.getDefinition();
        }

        // Build the result partitioned table
        return new PartitionedTableImpl(
                resultTable,
                keyColumnNames,
                constituentColumnName,
                resultConstituentDefinition,
                constituentChangesPermitted,
                true);
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

        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        final LivenessManager enclosingScope = LivenessScopeStack.peek();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            // Perform the transformation
            final Table joined = table.join(
                    other.table(),
                    JoinMatch.from(keyColumnNames),
                    List.of(JoinAddition.of(RHS_CONSTITUENT,
                            ColumnName.of(other.constituentColumnName()))));
            resultTable = joined.update(new BiTableTransformationColumn(
                    constituentColumnName, RHS_CONSTITUENT.name(), transformer));
            enclosingScope.manage(resultTable);

            // Make sure we have a valid result constituent definition
            final Table emptyConstituent1 = emptyConstituent(constituentDefinition);
            final Table emptyConstituent2 = emptyConstituent(other.constituentDefinition());
            final Table resultEmptyConstituent = transformer.apply(emptyConstituent1, emptyConstituent2);
            resultConstituentDefinition = resultEmptyConstituent.getDefinition();
        }

        // Build the result partitioned table
        return new PartitionedTableImpl(
                resultTable,
                keyColumnNames,
                constituentColumnName,
                resultConstituentDefinition,
                constituentChangesPermitted || other.constituentChangesPermitted(),
                true);
    }

    // TODO (https://github.com/deephaven/deephaven-core/issues/2368): Consider adding transformWithKeys support

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

    private static final class ValidateConstituents implements QueryTable.MemoizableOperation<QueryTable> {

        private final QueryTable parent;
        private final String constituentColumnName;
        private final TableDefinition constituentDefinition;

        private ValidateConstituents(
                @NotNull final QueryTable parent,
                @NotNull final String constituentColumnName,
                @NotNull final TableDefinition constituentDefinition) {
            this.parent = parent;
            this.constituentColumnName = constituentColumnName;
            this.constituentDefinition = constituentDefinition;
        }

        @Override
        public String getDescription() {
            return "validate partitioned table constituents for " + parent.getDescription();
        }

        @Override
        public String getLogPrefix() {
            return "validate-constituents-for-{" + parent.getDescription() + '}';
        }

        @Override
        public Result<QueryTable> initialize(final boolean usePrev, final long beforeClock) {
            final ColumnSource<Table> constituentColumnSource = parent.getColumnSource(constituentColumnName);
            try (final RowSequence prevRows = usePrev ? parent.getRowSet().copyPrev() : null) {
                final RowSequence rowsToCheck = usePrev ? prevRows : parent.getRowSet();
                validateConstituents(constituentDefinition, constituentColumnSource, rowsToCheck);
            }
            final QueryTable child = parent.getSubTable(parent.getRowSet(), parent.getModifiedColumnSetForUpdates());
            parent.propagateFlatness(child);
            parent.copyAttributes(child, a -> true);
            return new Result<>(child, new BaseTable.ListenerImpl(getDescription(), parent, child) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    validateConstituents(constituentDefinition, constituentColumnSource, upstream.modified());
                    validateConstituents(constituentDefinition, constituentColumnSource, upstream.added());
                    super.onUpdate(upstream);
                }
            });
        }

        @Override
        public MemoizedOperationKey getMemoizedOperationKey() {
            return new ValidateConstituentsMemoizationKey(constituentColumnName, constituentDefinition);
        }
    }

    private static void validateConstituents(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final ColumnSource<Table> constituentSource,
            @NotNull final RowSequence rowsToValidate) {
        try (final ChunkSource.GetContext getContext = constituentSource.makeGetContext(DEFAULT_CHUNK_SIZE);
                final RowSequence.Iterator rowsIterator = rowsToValidate.getRowSequenceIterator()) {
            final RowSequence sliceRows = rowsIterator.getNextRowSequenceWithLength(DEFAULT_CHUNK_SIZE);
            final ObjectChunk<Table, ? extends Values> sliceConstituents =
                    constituentSource.getChunk(getContext, sliceRows).asObjectChunk();
            final int sliceSize = sliceConstituents.size();
            for (int sci = 0; sci < sliceSize; ++sci) {
                final Table constituent = sliceConstituents.get(sci);
                if (constituent == null) {
                    throw new IllegalStateException("Encountered null constituent");
                }
                constituentDefinition.checkMutualCompatibility(constituent.getDefinition());
            }
        }
    }

    private static final class ValidateConstituentsMemoizationKey extends MemoizedOperationKey {

        private final String constituentColumnName;
        private final TableDefinition constituentDefinition;

        private final int hashCode;

        private ValidateConstituentsMemoizationKey(
                @NotNull final String constituentColumnName,
                @NotNull final TableDefinition constituentDefinition) {
            this.constituentColumnName = constituentColumnName;
            this.constituentDefinition = constituentDefinition;
            final MutableInt hashAccumulator = new MutableInt(31 + constituentColumnName.hashCode());
            constituentDefinition.getColumnStream().map(ColumnDefinition::getName).sorted().forEach(
                    cn -> hashAccumulator.setValue(31 * hashAccumulator.intValue() + cn.hashCode()));
            hashCode = hashAccumulator.intValue();
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final ValidateConstituentsMemoizationKey that = (ValidateConstituentsMemoizationKey) other;
            return constituentColumnName.equals(that.constituentColumnName)
                    && constituentDefinition.equalsIgnoreOrder(that.constituentDefinition);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }
}

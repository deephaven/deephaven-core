/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.Pair;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessManager;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import io.deephaven.engine.table.iterators.ObjectColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.select.MatchFilter.MatchType.Inverted;
import static io.deephaven.engine.table.iterators.ColumnIterator.*;

/**
 * {@link PartitionedTable} implementation.
 */
public class PartitionedTableImpl extends LivenessArtifact implements PartitionedTable {

    private static final String RHS_CONSTITUENT = "__RHS_CONSTITUENT__";

    private final Table table;
    private final Set<String> keyColumnNames;
    private final boolean uniqueKeys;
    private final String constituentColumnName;
    private final TableDefinition constituentDefinition;
    private final boolean constituentChangesPermitted;

    private volatile WeakReference<QueryTable> memoizedMerge;

    /**
     * @see PartitionedTableFactory#of(Table, Collection, boolean, String, TableDefinition, boolean) Factory method that
     *      delegates to this method
     * @apiNote Only engine-internal tools should call this constructor directly
     */
    public PartitionedTableImpl(
            @NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            final boolean uniqueKeys,
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
        this.uniqueKeys = uniqueKeys;
        this.constituentColumnName = constituentColumnName;
        this.constituentDefinition = constituentDefinition;
        this.constituentChangesPermitted = constituentChangesPermitted && table.isRefreshing();
    }

    @Override
    public String toString() {
        return "PartitionedTable for " + table.getDescription();
    }

    @ConcurrentMethod
    @Override
    public Table table() {
        return table;
    }

    @ConcurrentMethod
    @Override
    public Set<String> keyColumnNames() {
        return keyColumnNames;
    }

    @ConcurrentMethod
    @Override
    public boolean uniqueKeys() {
        return uniqueKeys;
    }

    @ConcurrentMethod
    @Override
    public String constituentColumnName() {
        return constituentColumnName;
    }

    @ConcurrentMethod
    @Override
    public TableDefinition constituentDefinition() {
        return constituentDefinition;
    }

    @ConcurrentMethod
    @Override
    public boolean constituentChangesPermitted() {
        return constituentChangesPermitted;
    }

    @ConcurrentMethod
    @Override
    public PartitionedTable.Proxy proxy(final boolean requireMatchingKeys, final boolean sanityCheckJoinOperations) {
        return PartitionedTableProxyImpl.of(this, requireMatchingKeys, sanityCheckJoinOperations);
    }

    @Override
    public Table merge() {
        QueryTable merged;
        WeakReference<QueryTable> localMemoizedMerge;
        if ((localMemoizedMerge = memoizedMerge) != null
                && Liveness.verifyCachedObjectForReuse(merged = localMemoizedMerge.get())) {
            return merged;
        }
        synchronized (this) {
            if ((localMemoizedMerge = memoizedMerge) != null
                    && Liveness.verifyCachedObjectForReuse(merged = localMemoizedMerge.get())) {
                return merged;
            }
            if (table.isRefreshing()) {
                UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
            }
            final UnionSourceManager unionSourceManager = new UnionSourceManager(this);
            merged = unionSourceManager.getResult();

            merged.setAttribute(Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE);
            if (!constituentChangesPermitted) {
                final Map<String, Object> sharedAttributes;
                try (final ObjectColumnIterator<Table> constituents =
                        table().objectColumnIterator(constituentColumnName)) {
                    sharedAttributes = computeSharedAttributes(constituents);
                }
                sharedAttributes.forEach(merged::setAttribute);
            }

            memoizedMerge = new WeakReference<>(merged);
        }
        return merged;
    }

    private Map<String, Object> computeSharedAttributes(@NotNull final Iterator<Table> constituents) {
        if (!constituents.hasNext()) {
            return Collections.emptyMap();
        }
        final Map<String, Object> candidates = new HashMap<>(constituents.next().getAttributes());
        while (constituents.hasNext()) {
            final Table constituent = constituents.next();
            final Iterator<Map.Entry<String, Object>> candidatesIter = candidates.entrySet().iterator();
            while (candidatesIter.hasNext()) {
                final Map.Entry<String, Object> candidate = candidatesIter.next();
                final String attrKey = candidate.getKey();
                final Object candidateValue = candidate.getValue();
                final boolean matches = constituent.hasAttribute(attrKey) &&
                        Objects.equals(constituent.getAttribute(attrKey), candidateValue);
                if (!matches) {
                    candidatesIter.remove();
                }
            }
            if (candidates.isEmpty()) {
                return Collections.emptyMap();
            }
        }
        return candidates;
    }

    @ConcurrentMethod
    @Override
    public PartitionedTableImpl filter(@NotNull final Collection<? extends Filter> filters) {
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
                uniqueKeys,
                constituentColumnName,
                constituentDefinition,
                constituentChangesPermitted || table.isRefreshing(),
                false);
    }

    @ConcurrentMethod
    @Override
    public PartitionedTable sort(@NotNull final Collection<SortColumn> sortColumns) {
        final boolean invalidSortColumn = sortColumns.stream()
                .map((final SortColumn sortColumn) -> sortColumn.column().name())
                .anyMatch((final String columnName) -> columnName.equals(constituentColumnName));
        if (invalidSortColumn) {
            throw new IllegalArgumentException("Unsupported sort on constituent column " + constituentColumnName
                    + " found in sort columns: " + sortColumns);
        }
        return new PartitionedTableImpl(
                table.sort(sortColumns),
                keyColumnNames,
                uniqueKeys,
                constituentColumnName,
                constituentDefinition,
                constituentChangesPermitted || table.isRefreshing(),
                false);
    }

    @ConcurrentMethod
    @Override
    public PartitionedTableImpl transform(final ExecutionContext executionContext,
            @NotNull final UnaryOperator<Table> transformer) {
        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        final LivenessManager enclosingScope = LivenessScopeStack.peek();
        try (final SafeCloseable ignored1 = executionContext == null ? null : executionContext.open();
                final SafeCloseable ignored2 = LivenessScopeStack.open()) {

            // Perform the transformation
            resultTable = table.update(new TableTransformationColumn(
                    constituentColumnName, executionContext, transformer));
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
                uniqueKeys,
                constituentColumnName,
                resultConstituentDefinition,
                constituentChangesPermitted,
                true);
    }

    @Override
    public PartitionedTableImpl partitionedTransform(
            @NotNull final PartitionedTable other,
            final ExecutionContext executionContext,
            @NotNull final BinaryOperator<Table> transformer) {
        // Check safety before doing any extra work
        if (table.isRefreshing() || other.table().isRefreshing()) {
            UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        }

        // Validate join compatibility
        final MatchPair[] joinPairs = matchKeyColumns(this, other);

        final Table resultTable;
        final TableDefinition resultConstituentDefinition;
        final LivenessManager enclosingScope = LivenessScopeStack.peek();
        try (final SafeCloseable ignored1 = executionContext == null ? null : executionContext.open();
                final SafeCloseable ignored2 = LivenessScopeStack.open()) {
            // Perform the transformation
            final MatchPair[] joinAdditions =
                    new MatchPair[] {new MatchPair(RHS_CONSTITUENT, other.constituentColumnName())};
            final Table joined = uniqueKeys
                    ? table.naturalJoin(other.table(), joinPairs, joinAdditions)
                            .where(new MatchFilter(Inverted, RHS_CONSTITUENT, (Object) null))
                    : table.join(other.table(), joinPairs, joinAdditions);
            resultTable = joined
                    .update(new BiTableTransformationColumn(
                            constituentColumnName, RHS_CONSTITUENT, executionContext, transformer))
                    .dropColumns(RHS_CONSTITUENT);
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
                uniqueKeys,
                constituentColumnName,
                resultConstituentDefinition,
                constituentChangesPermitted || other.constituentChangesPermitted(),
                true);
    }

    // TODO (https://github.com/deephaven/deephaven-core/issues/2368): Consider adding transformWithKeys support

    @ConcurrentMethod
    @Override
    public Table constituentFor(@NotNull final Object... keyColumnValues) {
        if (keyColumnValues.length != keyColumnNames.size()) {
            throw new IllegalArgumentException(
                    "Key count mismatch: expected one key column value for each key column name in " + keyColumnNames
                            + ", instead received " + Arrays.toString(keyColumnValues));
        }
        final int numKeys = keyColumnValues.length;
        final List<MatchFilter> filters = new ArrayList<>(numKeys);
        final String[] keyColumnNames = keyColumnNames().toArray(String[]::new);
        for (int kci = 0; kci < numKeys; ++kci) {
            filters.add(new MatchFilter(keyColumnNames[kci], keyColumnValues[kci]));
        }
        return LivenessScopeStack.computeEnclosed(() -> {
                    final Table[] matchingConstituents = filter(filters).snapshotConstituents();
                    final int matchingCount = matchingConstituents.length;
                    if (matchingCount > 1) {
                        throw new UnsupportedOperationException(
                                "Result size mismatch: expected 0 or 1 results, instead found " + matchingCount);
                    }
                    return matchingCount == 1 ? matchingConstituents[0] : null;
                },
                table::isRefreshing,
                constituent -> constituent != null && constituent.isRefreshing());
    }

    @ConcurrentMethod
    @Override
    public Table[] constituents() {
        return LivenessScopeStack.computeArrayEnclosed(
                this::snapshotConstituents,
                table::isRefreshing,
                constituent -> constituent != null && constituent.isRefreshing());
    }

    private Table[] snapshotConstituents() {
        if (constituentChangesPermitted) {
            final MutableObject<Table[]> resultHolder = new MutableObject<>();
            ConstructSnapshot.callDataSnapshotFunction(
                    "PartitionedTable.constituents(): ",
                    ConstructSnapshot.makeSnapshotControl(false, true, (QueryTable) table.coalesce()),
                    (final boolean usePrev, final long beforeClockValue) -> {
                        resultHolder.setValue(fetchConstituents(usePrev));
                        return true;
                    });
            return resultHolder.getValue();
        } else {
            return fetchConstituents(false);
        }
    }

    private Table[] fetchConstituents(final boolean usePrev) {
        try (final RowSet prevRowSet = usePrev ? table.getRowSet().copyPrev() : null) {
            final RowSequence rowsToFetch = usePrev
                    ? prevRowSet
                    : table.getRowSet();
            final ColumnSource<Table> constituentColumnSource = table.getColumnSource(constituentColumnName);
            final ChunkSource<Values> chunkSourceToFetch = usePrev
                    ? constituentColumnSource.getPrevSource()
                    : constituentColumnSource;
            try (final Stream<Table> constituentStream =
                    new ObjectColumnIterator<Table>(chunkSourceToFetch, rowsToFetch).stream()) {
                return constituentStream.toArray(Table[]::new);
            }
        }
    }

    /**
     * Validate that {@code lhs} and {@code rhs} have compatible key columns, allowing
     * {@link #partitionedTransform(PartitionedTable, BinaryOperator)}. Compute the matching pairs of key column names.
     *
     * @param lhs The first partitioned table
     * @param rhs The second partitioned table
     * @return {@link MatchPair Match pairs} linking {@code lhs}'s key column names with {@code rhs}'s key column names
     *         in the order dictated by {@code lhs}
     * @throws IllegalArgumentException If the key columns are mismatched
     */
    static MatchPair[] matchKeyColumns(@NotNull final PartitionedTable lhs, @NotNull final PartitionedTable rhs) {
        if (lhs.keyColumnNames().size() != rhs.keyColumnNames().size()) {
            throw new IllegalArgumentException("Incompatible partitioned table input for partitioned transform; "
                    + "key column sets don't contain the same names or the same number of columns: "
                    + "first has " + lhs.keyColumnNames() + ", second has " + rhs.keyColumnNames());
        }
        final MatchPair[] keyColumnNamePairs;
        if (lhs.keyColumnNames().equals(rhs.keyColumnNames())) {
            keyColumnNamePairs = lhs.keyColumnNames().stream()
                    .map(cn -> new MatchPair(cn, cn)).toArray(MatchPair[]::new);
        } else {
            final String[] lhsKeyColumns = lhs.keyColumnNames().toArray(String[]::new);
            final String[] rhsKeyColumns = rhs.keyColumnNames().toArray(String[]::new);
            keyColumnNamePairs = IntStream.range(0, lhsKeyColumns.length)
                    .mapToObj(ci -> new MatchPair(lhsKeyColumns[ci], rhsKeyColumns[ci])).toArray(MatchPair[]::new);
        }
        final String typeMismatches = Arrays.stream(keyColumnNamePairs)
                .map(namePair -> new Pair<>(
                        lhs.table().getDefinition().getColumn(namePair.leftColumn()),
                        rhs.table().getDefinition().getColumn(namePair.rightColumn())))
                .filter(defPair -> defPair.getFirst().getDataType() != defPair.getSecond().getDataType()
                        || defPair.getFirst().getComponentType() != defPair.getSecond().getComponentType())
                .map(defPair -> defPair.getFirst().describeForCompatibility() + " doesn't match "
                        + defPair.getSecond().describeForCompatibility())
                .collect(Collectors.joining(", "));
        if (!typeMismatches.isEmpty()) {
            throw new IllegalArgumentException("Incompatible partitioned table input for partitioned transform; "
                    + "key column definitions don't match: " + typeMismatches);
        }
        return keyColumnNamePairs;
    }

    private static Table emptyConstituent(@NotNull final TableDefinition constituentDefinition) {
        // noinspection resource
        return new QueryTable(
                constituentDefinition,
                RowSetFactory.empty().toTracking(),
                NullValueColumnSource.createColumnSourceMap(constituentDefinition));
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
            final QueryTable child = parent.getSubTable(
                    parent.getRowSet(), parent.getModifiedColumnSetForUpdates(), parent.getAttributes());
            parent.propagateFlatness(child);
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
                constituentDefinition.checkMutualCompatibility(constituent.getDefinition(), "expected", "constituent");
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

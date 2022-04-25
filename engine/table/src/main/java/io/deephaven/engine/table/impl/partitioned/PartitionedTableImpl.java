package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link PartitionedTable} implementation.
 */
public class PartitionedTableImpl extends LivenessArtifact implements PartitionedTable {



    private final Table table;
    private final Set<String> keyColumnNames;
    private final String constituentColumnName;
    private final TableDefinition constituentDefinition;

    /**
     * @see io.deephaven.engine.table.PartitionedTableFactory#of(Table, Set, String, TableDefinition) Factory method
     *      that delegates to this method
     */
    PartitionedTableImpl(@NotNull final Table table,
            @NotNull final Collection<String> keyColumnNames,
            @NotNull final String constituentColumnName,
            @NotNull final TableDefinition constituentDefinition) {
        this.table = table;
        this.keyColumnNames = new LinkedHashSet<>(keyColumnNames);
        this.constituentColumnName = constituentColumnName;
        this.constituentDefinition = constituentDefinition;
    }

    @Override
    public Table table() {
        return table;
    }

    @Override
    public PartitionedTable.Proxy proxy() {
        return PartitionedTableProxyHandler.proxyFor(this);
    }

    @Override
    public Table merge() {

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
        return new PartitionedTableImpl(table.where(whereFilters),
                keyColumnNames, constituentColumnName, constituentDefinition);
    }

    @Override
    public PartitionedTable transform(@NotNull final Function<Table, Table> transformer) {

    }

    @Override
    public PartitionedTable partitionedTransform(
            @NotNull final PartitionedTable other,
            @NotNull final BiFunction<Table, Table, Table> transformer) {

    }

    // TODO-RWC: Add ticket for this
    // TODO (insert ticket here): Support "PartitionedTable withCombinedKeys(String keyColumnName)" for combining
    //     multiple key columns into a compound key column using the tuple library, and then add "transformWithKeys"
    //     support.

    /**
     * {@link SelectColumn} implementation to wrap transformer functions for {@link #transform(Function)} and
     * {@link #partitionedTransform(PartitionedTable, BiFunction)}.
     */
    private abstract class BaseTableTransformation implements SelectColumn {

        private BaseTableTransformation() {
        }

        @Override
        public final List<String> initInputs(@NotNull final Table table) {
            return initInputs(table.getRowSet(), table.getColumnSourceMap());
        }

        @Override
        public List<String> initInputs(@NotNull final TrackingRowSet rowSet,
                                       @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
            return null;
        }

        @Override
        public List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
            return null;
        }

        @Override
        public final Class<?> getReturnedType() {
            return Table.class;
        }

        @Override
        public List<String> getColumns() {
            return null;
        }

        @Override
        public final List<String> getColumnArrays() {
            return Collections.emptyList();
        }

        @NotNull
        @Override
        public ColumnSource<?> getDataView() {
            return null;
        }

        @NotNull
        @Override
        public ColumnSource<?> getLazyView() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public MatchPair getMatchPair() {
            return null;
        }

        @Override
        public WritableColumnSource<?> newDestInstance(final long size) {
            return SparseArrayColumnSource.getSparseMemoryColumnSource(size, Table.class);
        }

        @Override
        public WritableColumnSource<?> newFlatDestInstance(final long size) {
            return InMemoryColumnSource.getImmutableMemoryColumnSource(size, Table.class, null);
        }

        @Override
        public boolean isRetain() {
            return false;
        }

        @Override
        public boolean disallowRefresh() {
            return false;
        }

        @Override
        public boolean isStateless() {
            return true;
        }

        @Override
        public SelectColumn copy() {
            return this;
        }
    }
}

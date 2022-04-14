package io.deephaven.engine.table.impl;

import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

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
    public TableOperations<? extends TableOperations, ? extends TableOperations> proxy() {

    }

    @Override
    public Table merge() {

    }

    @Override
    public PartitionedTable filter(@NotNull final Collection<? extends Filter> filters) {

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
     * {@link TableOperations} that proxies methods to the component tables of a partitioned table via
     * {@link Table#update}.
     * <p>
     * For example, an operation like {@code where(ColumnA=ValueB)} against a proxy with a component table column named
     * {@code Components} is implemented equivalently to:
     *
     * <pre>
     * Proxy.of(getUnderlyingPartitionedTable().update("Components=Components.where(ColumnA=ValueB)"), "Components")
     * </pre>
     * <p>
     * Note that the result partitioned table can be retrieved using {@link #getUnderlyingPartitionedTable()}.
     */
    public interface Proxy extends TableOperations<Proxy, Table> {

        static Proxy of(@NotNull final Table underlying, @NotNull final String componentColumnname) {
            return (Proxy) java.lang.reflect.Proxy.newProxyInstance(
                    Proxy.class.getClassLoader(),
                    new Class[] {Proxy.class},
                    new ProxyHandler(underlying, componentColumnname));
        }

        /**
         * @return The underlying partitioned table
         */
        Table getUnderlyingPartitionedTable();
    }

    private static class ProxyHandler extends LivenessArtifact implements InvocationHandler {

        private final Table underlying;
        private final String componentColumnName;

        private final String description;

        public ProxyHandler(@NotNull final Table underlying, @NotNull final String componentColumnName) {
            this.underlying = underlying;
            this.componentColumnName = componentColumnName;
            checkPartitionTable(underlying, componentColumnName);
            description = "PartitionedTable{" + underlying + ',' + componentColumnName + '}';
            manage(underlying);
        }

        public Table getUnderlyingPartitionedTable() {
            return underlying;
        }

        @Override
        public String toString() {
            return description;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }
    }

    private static class TableTransformation implements SelectColumn {

        private final Method method;

        private TableTransformation(@NotNull final Method method) {
            this.method = method;
        }

        @Override
        public List<String> initInputs(Table table) {
            return null;
        }

        @Override
        public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
            return null;
        }

        @Override
        public List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
            return null;
        }

        @Override
        public Class<?> getReturnedType() {
            return null;
        }

        @Override
        public List<String> getColumns() {
            return null;
        }

        @Override
        public List<String> getColumnArrays() {
            return null;
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
        public WritableColumnSource<?> newDestInstance(long size) {
            return null;
        }

        @Override
        public WritableColumnSource<?> newFlatDestInstance(long size) {
            return null;
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

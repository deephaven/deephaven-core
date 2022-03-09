package io.deephaven.engine.table.impl;

import io.deephaven.api.TableOperations;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.SelectColumn;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Tools for working with partitioned tables. A partitioned table is a table with one or more columns containing
 * like-defined component tables.
 * 
 * @see Table#partitionBy
 * @see io.deephaven.api.agg.Partition
 */
public class PartitionedTableTools {

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

    private static class ProxyHandler implements InvocationHandler {

        private final Table underlying;
        private final String componentColumnName;

        private final String description;

        public ProxyHandler(@NotNull final Table underlying, @NotNull final String componentColumnName) {
            this.underlying = underlying;
            this.componentColumnName = componentColumnName;
            final ColumnDefinition<?> componentColumnDefinition =
                    underlying.getDefinition().getColumn(componentColumnName);
            if (componentColumnDefinition == null) {
                throw new IllegalArgumentException("Underlying table " + underlying
                        + " has no column named " + componentColumnName);
            }
            if (!Table.class.isAssignableFrom(componentColumnDefinition.getDataType())) {
                throw new IllegalArgumentException("Component column " + componentColumnName
                        + " has unsupported data type " + componentColumnDefinition.getDataType());
            }
            description = "PartitionedTableTools{" + underlying + ',' + componentColumnName + '}';
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

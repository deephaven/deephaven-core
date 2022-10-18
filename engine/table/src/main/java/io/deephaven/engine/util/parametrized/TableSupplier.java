/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util.parametrized;

import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.ScriptApi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.function.Function;

/**
 * TableSupplier creates a Proxy to a Table with a list of Table operations to be applied when a filter method is
 * called.<br>
 */
@ScriptApi
public class TableSupplier extends LivenessArtifact implements InvocationHandler {

    private static final String FILTER_OPERATION_PREFIX = "where";
    private static final Map<Method, InvocationHandler> HIJACKED_DELEGATIONS = new HashMap<>();
    static {
        try {
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("coalesce"),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).coalesce());
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("hasColumns", Collection.class),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy))
                            .hasColumns((Collection<String>) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("hasColumns", String[].class), (proxy, method,
                    args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).hasColumns((String[]) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("partitionBy", String[].class), (proxy, method,
                    args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).partitionBy((String[]) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("partitionBy", boolean.class, String[].class),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy))
                            .partitionBy((Boolean) args[0], (String[]) args[1]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("apply", Function.class),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy))
                            .apply((Function) args[0], (Table) proxy));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("setAttribute", String.class, Object.class),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy))
                            .setAttribute((String) args[0], args[1]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttribute", String.class), (proxy, method,
                    args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).getAttribute((String) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttributeKeys"),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).getAttributeNames());
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("hasAttribute", String.class), (proxy, method,
                    args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).hasAttribute((String) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttributes"),
                    (proxy, method, args) -> ((TableSupplier) Proxy.getInvocationHandler(proxy)).getAttributes());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Class[] PROXY_INTERFACES = new Class[] {Table.class};

    /**
     * Checks if a method is a filter operation.
     *
     * @param method the method to check
     * @return true if that method is a filter operation, false otherwise
     */
    private static boolean isFilterOperation(Method method) {
        return method.getName().startsWith(FILTER_OPERATION_PREFIX);
    }

    private static final Logger log = LoggerFactory.getLogger(TableSupplier.class);

    /**
     * The source table used to generate the supplied table
     */
    private final Table sourceTable;

    /**
     * An empty table with all operations applied to it in order to support methods like hasColumn
     */
    private final Table appliedEmptyTable;

    /**
     * The operations to apply when a table is being supplied
     */
    private final List<Operation> tableOperations;

    /**
     * Once the user is done adding operations, the supplier should be marked as complete
     */
    private final boolean isComplete;

    /**
     * A map of table attributes. This is necessary for ACL support.
     */
    private final Map<String, Object> attributes = new HashMap<>();

    /**
     * Use to start the construction of a Table Supplier.
     *
     * @param sourceTable the source table
     * @return a Proxy that will supply a table
     */
    @ScriptApi
    public static Table build(Table sourceTable) {
        return (Table) Proxy.newProxyInstance(TableSupplier.class.getClassLoader(), PROXY_INTERFACES,
                new TableSupplier(sourceTable, TableTools.newTable(sourceTable.getDefinition()),
                        Collections.emptyList(), false));
    }

    /**
     * Sets a Table Supplier to be complete. This means that the supplier will generate a table the next time a filter
     * operation is called. This method has no affect on Tables that are not suppliers.
     *
     * @param maybeSupplier a Table that may be a supplier
     * @return a completed Supplier or unaltered Table
     */
    @ScriptApi
    public static Table complete(Table maybeSupplier) {
        return callTableSupplierMethod(maybeSupplier, TableSupplier::complete);
    }

    /**
     * Gets an empty version of the supplied table with all current operations applied to it. If the Table is not a
     * Table Supplier then this will return the table unaltered.
     *
     * @param maybeSupplier a Table that may be a supplier
     * @return an applied empty table or an unaltered table
     */
    @ScriptApi
    public static Table getAppliedEmptyTable(Table maybeSupplier) {
        return callTableSupplierMethod(maybeSupplier, TableSupplier::getAppliedEmptyTable);
    }

    private static Table callTableSupplierMethod(Table maybeSupplier,
            java.util.function.Function<TableSupplier, Table> method) {
        if (maybeSupplier == null) {
            return null;
        }

        try {
            final InvocationHandler handler = Proxy.getInvocationHandler(maybeSupplier);
            if (handler instanceof TableSupplier) {
                return method.apply((TableSupplier) handler);
            }
            return maybeSupplier;
        } catch (IllegalArgumentException e) {
            return maybeSupplier;
        }
    }

    private TableSupplier(Table sourceTable, Table appliedEmptyTable, List<Operation> tableOperations,
            boolean isComplete) {
        this.sourceTable = sourceTable;
        this.appliedEmptyTable = appliedEmptyTable;
        // This is intended to be a copy
        this.tableOperations = new ArrayList<>(tableOperations);
        this.isComplete = isComplete;
        setAttribute(Table.NON_DISPLAY_TABLE, true);
    }

    private Table complete() {
        log.info().append("TableSupplier setting complete").endl();
        final TableSupplier copy = new TableSupplier(sourceTable, appliedEmptyTable, tableOperations, true);
        return (Table) Proxy.newProxyInstance(TableSupplier.class.getClassLoader(), PROXY_INTERFACES, copy);
    }

    private Table getAppliedEmptyTable() {
        return appliedEmptyTable;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Check for methods we explicitly want to hijack
        final InvocationHandler handler = HIJACKED_DELEGATIONS.get(method);
        if (handler != null) {
            return handler.invoke(proxy, method, args);
        }

        // Methods that produce a Table should be deferred
        if (Table.class.isAssignableFrom(method.getReturnType())) {
            return deferOrExecute(method, args);
        }

        // All PartitionedTable operations should be hijacked
        if (PartitionedTable.class.isAssignableFrom(method.getReturnType())) {
            throw new IllegalStateException(
                    "TableSupplier partitionBy methods should be hijacked but invoked " + method.getName());
        }

        log.info().append("TableSupplier invoking on applied empty table ").append(method.getName()).endl();

        // Let the source table handle everything else
        return method.invoke(appliedEmptyTable, args);
    }

    private Table deferOrExecute(Method method, Object[] args)
            throws InvocationTargetException, IllegalAccessException {
        if (isComplete && isFilterOperation(method)) {
            return execute(method, args);
        } else {
            return defer(method, args);
        }
    }

    private Table defer(Method method, Object[] args) throws InvocationTargetException, IllegalAccessException {
        log.info().append("TableSupplier defer ").append(method.getName()).endl();
        // Defer the table operation by adding to a copy of this table
        final TableSupplier copy = new TableSupplier(sourceTable, (Table) method.invoke(appliedEmptyTable, args),
                tableOperations, isComplete);
        copy.tableOperations.add(new Operation(method, args));
        return (Table) Proxy.newProxyInstance(TableSupplier.class.getClassLoader(), PROXY_INTERFACES, copy);
    }

    private Table execute(Method method, Object[] args) {
        log.info().append("TableSupplier execute ").append(method.getName()).endl();
        try {
            Table result = (Table) method.invoke(sourceTable, args);
            result = applyOperations(result);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table applyOperations(Table table) throws IllegalAccessException, InvocationTargetException {
        for (Operation operation : tableOperations) {
            table = (Table) operation.method.invoke(table, operation.args);
        }
        return table;
    }

    // region Hijacked Operations

    /**
     * Coalesce will apply all the table operations at any point in the supplier's construction. The supplier need not
     * be complete nor does coalesce require a filter operation.
     *
     * @return a coalesced Table from the supplier
     */
    private Table coalesce() {
        log.info().append("TableSupplier applying coalesce").endl();
        try {
            return applyOperations(sourceTable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This hasColumns implementation is intentionally permissive. It returns true if the table supplier a column prior
     * to applying operations or after applying operations. This allows various one click implementations to succeed
     * when they check for columns.
     *
     * @param columnNames the column names to check
     * @return true if the table supplier has each column either before or after operations, false otherwise
     */
    private boolean hasColumns(Collection<String> columnNames) {
        // Check that either the "before table" or "after table" has the column
        for (String name : columnNames) {
            if (!sourceTable.hasColumns(name) && !appliedEmptyTable.hasColumns(name)) {
                // If neither table has the column than this is false
                return false;
            }
        }
        return true;
    }

    private boolean hasColumns(final String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return hasColumns(Arrays.asList(columnNames));
    }

    private PartitionedTable partitionBy(boolean dropKeys, String... columnNames) {
        return sourceTable.partitionBy(dropKeys, columnNames).transform((final Table constituent) -> {
            try {
                return applyOperations(constituent);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new UncheckedTableException(e);
            }
        });
    }

    private PartitionedTable partitionBy(String... columnNames) {
        return partitionBy(false, columnNames);
    }

    private Object apply(Function<Table, Object> function, Table table) {
        return function.apply(table);
    }

    private Void setAttribute(String key, Object object) {
        // These are the only supported attributes for Table Supplier
        if (Table.NON_DISPLAY_TABLE.equals(key)) {
            attributes.put(key, object);
        }
        return null;
    }

    private Object getAttribute(String key) {
        return attributes.get(key);
    }

    private Set<String> getAttributeNames() {
        return attributes.keySet();
    }

    private boolean hasAttribute(String name) {
        return attributes.containsKey(name);
    }

    private Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    // endregion Hijacked Operations

    /**
     * Convenience class for storing a method and its arguments together.
     */
    private static class Operation {
        private final Method method;
        private final Object[] args;

        Operation(Method method, Object[] args) {
            this.method = method;
            this.args = args;
        }
    }
}

package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.TimeZone;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * {@link PartitionedTable.Proxy} {@link java.lang.reflect.InvocationHandler} implementation.
 */
@SuppressWarnings("SuspiciousInvocationHandlerImplementation")
public class PartitionedTableProxyHandler extends LivenessArtifact implements InvocationHandler {

    private static final ColumnName FOUND_IN = ColumnName.of("__FOUND_IN__");
    private static final ColumnName ENCLOSING_CONSTITUENT = ColumnName.of("__ENCLOSING_CONSTITUENT__");

    private static final Map<Method, InvocationHandler> DIRECT_DELEGATIONS;
    static {
        final Map<Method, InvocationHandler> directDelegations = new HashMap<>();
        try {
            directDelegations.put(PartitionedTable.Proxy.class.getMethod("target"),
                    (proxy, method, args) -> ((PartitionedTableProxyHandler) Proxy.getInvocationHandler(proxy)).target);
            directDelegations.put(Object.class.getMethod("hashCode"),
                    (proxy, method, args) -> Proxy.getInvocationHandler(proxy).hashCode());
            directDelegations.put(Object.class.getMethod("equals"),
                    (proxy, method, args) -> Proxy.getInvocationHandler(proxy).equals(args[0]));
            directDelegations.put(Object.class.getMethod("toString"),
                    (proxy, method, args) -> Proxy.getInvocationHandler(proxy).toString());
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Missing expected method", e);
        }
        DIRECT_DELEGATIONS = Collections.unmodifiableMap(directDelegations);
    }

    private static final Set<String> JOIN_METHOD_NAMES = Set.of("join", "naturalJoin", "exactJoin", "aj", "raj");
    private static final Set<String> INEXACT_JOIN_METHOD_NAMES = Set.of("aj", "raj");

    /**
     * The underlying target {@link PartitionedTable}.
     */
    private final PartitionedTable target;
    private final boolean requireMatchingKeys;
    private final boolean sanityCheckJoins;

    private PartitionedTableProxyHandler(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
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
        final PartitionedTableProxyHandler that = (PartitionedTableProxyHandler) other;
        return requireMatchingKeys == that.requireMatchingKeys
                && sanityCheckJoins == that.sanityCheckJoins
                && target.equals(that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, requireMatchingKeys, sanityCheckJoins);
    }

    @Override
    public String toString() {
        return "PartitionedTable.Proxy for " + target.table().getDescription();
    }

    /**
     * Make a {@link PartitionedTable.Proxy proxy} to the supplied {@code target}.
     *
     * @param target The target {@link PartitionedTable} whose constituents should be used when proxying
     *        {@link io.deephaven.api.TableOperations table operations}
     * @param requireMatchingKeys As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @param sanityCheckJoins As in {@link PartitionedTable#proxy(boolean, boolean)}
     * @return A {@link PartitionedTable.Proxy proxy} to {@code target}
     */
    public static PartitionedTable.Proxy proxyFor(
            @NotNull final PartitionedTable target,
            final boolean requireMatchingKeys,
            final boolean sanityCheckJoins) {
        return (PartitionedTable.Proxy) Proxy.newProxyInstance(PartitionedTableProxyHandler.class.getClassLoader(),
                new Class[] {PartitionedTable.Proxy.class},
                new PartitionedTableProxyHandler(target, requireMatchingKeys, sanityCheckJoins));
    }

    @Override
    public Object invoke(
            @NotNull final Object proxy,
            @NotNull final Method method,
            @Nullable final Object[] args) throws Throwable {
        final InvocationHandler directHandler = DIRECT_DELEGATIONS.get(method);
        if (directHandler != null) {
            return directHandler.invoke(proxy, method, args);
        }

        if (method.getDeclaringClass() != TableOperations.class) {
            throw new UnsupportedOperationException("Unexpected declaring class for method " + method);
        }
        if (method.getReturnType() != TableOperations.class) {
            throw new UnsupportedOperationException("Unexpected return type for method " + method);
        }

        int tableArgumentIndex = -1;
        final Class<?>[] parameterTypes = method.getParameterTypes();
        for (int ii = 0; ii < parameterTypes.length; ii++) {
            if (TableOperations.class.isAssignableFrom(parameterTypes[ii])) {
                if (tableArgumentIndex >= 0) {
                    throw new UnsupportedOperationException(
                            "Unexpected method with multiple TableOperations arguments: " + method);
                }
                tableArgumentIndex = ii;
            }
        }
        final PartitionedTable.Proxy proxyArg =
                args == null || tableArgumentIndex < 0 || !(args[tableArgumentIndex] instanceof PartitionedTable.Proxy)
                        ? null
                        : (PartitionedTable.Proxy) args[tableArgumentIndex];

        if (proxyArg == null) {
            return QueryPerformanceRecorder.withNugget("PartitionedTableProxyHandler-" + method.getName(), () -> {
                final PartitionedTable resultTarget = target.transform(table -> {
                    try {
                        return (Table) method.invoke(table, args);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new UncheckedDeephavenException(e);
                    }
                });
                return proxyFor(resultTarget, requireMatchingKeys, sanityCheckJoins);
            });
        }
        throw new UnsupportedOperationException("TODO-RWC");
    }

    /**
     * Get a table of keys that are uniquely in only {@code lhs} or {@code rhs}, with an additional column identifying
     * the table where the key was encountered.
     *
     * @param lhs The left-hand-side (first) partitioned table
     * @param rhs The right-hand-side (second) partitioned table
     * @return A table of keys that are uniquely in only one of the input partitioned tables
     */
    private static Table uniqueKeysTable(
            @NotNull final PartitionedTable lhs,
            @NotNull final PartitionedTable rhs) {
        UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();

        final Table lhsKeys = lhs.table()
                .selectDistinct(lhs.keyColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))
                .updateView(new ConstantColumn<>(FOUND_IN.name(), String.class, "first"));
        final Table rhsKeys = rhs.table()
                .selectDistinct(rhs.keyColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))
                .updateView(new ConstantColumn<>(FOUND_IN.name(), String.class, "second"));
        final Table unionedKeys = TableTools.merge(lhsKeys, rhsKeys);
        final Table unionedKeysWithUniqueAgg = unionedKeys.aggAllBy(AggSpec.unique(), lhs.keyColumnNames());
        return unionedKeysWithUniqueAgg.where(Filter.isNotNull(FOUND_IN));
        // if (!uniqueKeys.isEmpty()) {
        // throw formatUniqueKeysException(uniqueKeys);
        // }
        // if (!uniqueKeys.isRefreshing()) {
        // return null;
        // }
        // return new MatchingKeysEnforcementListener(lhs, rhs, uniqueKeys);
    }

    private static class MatchingKeysEnforcementListener extends InstrumentedTableUpdateListenerAdapter {
        private final Table uniqueKeys;

        public MatchingKeysEnforcementListener(
                @NotNull final PartitionedTable lhs,
                @NotNull final PartitionedTable rhs,
                @NotNull final Table uniqueKeys) {
            super("Matching keys enforcement listener for " + lhs.table().getDescription()
                    + ", and " + rhs.table().getDescription(), uniqueKeys, false);
            this.uniqueKeys = uniqueKeys;
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            if (!uniqueKeys.isEmpty()) {
                throw formatUniqueKeysException(uniqueKeys);
            }
        }
    }

    private static IllegalArgumentException formatUniqueKeysException(@NotNull final Table uniqueKeys) {
        return new IllegalArgumentException("Partitioned table arguments have unique keys:\n"
                + tableToString(uniqueKeys, 10));
    }

    /**
     * Get a table of join keys that are found in more than one constituent table in {@code input}.
     *
     * @param input The input partitioned table
     * @param keyColumnNames The exact match key column names for the join operation
     * @return A table of join keys that are found in more than one constituent table in {@code input}
     */
    private static Table overlappingJoinKeysTable(
            @NotNull final PartitionedTable input,
            @NotNull final String[] keyColumnNames) {
        final PartitionedTable stamped = input.transform(table -> table
                .updateView(new ConstantColumn<>(ENCLOSING_CONSTITUENT.name(), Table.class, table)));
        final Table merged = stamped.merge();
        final Table mergedWithUniqueAgg = merged.aggAllBy(AggSpec.unique(), keyColumnNames);
        final Table overlappingKeys = mergedWithUniqueAgg.where(Filter.isNull(ENCLOSING_CONSTITUENT));
        return overlappingKeys.view(keyColumnNames);
    }

    private static class JoinSanityEnforcementListener extends InstrumentedTableUpdateListenerAdapter {

        private final String inputTableDescription;
        private final Table overlappingJoinKeys;

        public JoinSanityEnforcementListener(
                @NotNull final PartitionedTable input,
                @NotNull final Table overlappingJoinKeys) {
            super("Join sanity enforcement listener for " + input.table().getDescription(), overlappingJoinKeys, false);
            inputTableDescription = input.table().getDescription();
            this.overlappingJoinKeys = overlappingJoinKeys;
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            if (!overlappingJoinKeys.isEmpty()) {
                throw formatOverlappingJoinKeysException(inputTableDescription, overlappingJoinKeys);
            }
        }
    }

    private static IllegalArgumentException formatOverlappingJoinKeysException(
            @NotNull final String inputDescription,
            @NotNull final Table overlappingKeys) {
        return new IllegalArgumentException("Partitioned table \"" + inputDescription
                + "\" has join keys found in multiple constituents:\n"
                + tableToString(overlappingKeys, 10));
    }

    private static String tableToString(@NotNull final Table table, final int maximumRows) {
        try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                final PrintStream printStream = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            TableTools.show(table, maximumRows, TimeZone.TZ_DEFAULT, printStream);
            printStream.flush();
            return bytes.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

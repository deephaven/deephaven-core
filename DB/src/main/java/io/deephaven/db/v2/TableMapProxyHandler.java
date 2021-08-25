package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.liveness.Liveness;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TableMapProxyHandler extends LivenessArtifact implements InvocationHandler {

    private static final Map<Method, InvocationHandler> HIJACKED_DELEGATIONS = new HashMap<>();
    private static final Set<Method> COALESCING_METHODS = new HashSet<>();
    private static final Set<String> JOIN_METHOD_NAMES = new HashSet<>();
    private static final Set<String> AJ_METHOD_NAMES = new HashSet<>();
    static {
        try {
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("size"), (proxy, method,
                args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy)).size());
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("coalesce"), (proxy, method,
                args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy)).coalesce());
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getDefinition"),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .getDefinition());
            HIJACKED_DELEGATIONS.put(
                TransformableTableMap.class.getMethod("asTable", boolean.class, boolean.class,
                    boolean.class),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .asTable((boolean) args[0], (boolean) args[1], (boolean) args[2]));
            HIJACKED_DELEGATIONS.put(TransformableTableMap.class.getMethod("merge"), (proxy, method,
                args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy)).merge());
            HIJACKED_DELEGATIONS.put(TransformableTableMap.class.getMethod("asTableMap"),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .asTableMap());
            HIJACKED_DELEGATIONS.put(TransformableTableMap.class.getMethod("asTableBuilder"),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .asTableBuilder());
            HIJACKED_DELEGATIONS.put(Object.class.getMethod("toString"),
                (proxy, method, args) -> Proxy.getInvocationHandler(proxy).toString());

            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttribute", String.class),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .getAttribute((String) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttributeNames"),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .getAttributeNames());
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("hasAttribute", String.class),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .hasAttribute((String) args[0]));
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttributes"),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .getAttributes(true, Collections.emptySet()));
            // noinspection unchecked
            HIJACKED_DELEGATIONS.put(Table.class.getMethod("getAttributes", Collection.class),
                (proxy, method, args) -> ((TableMapProxyHandler) Proxy.getInvocationHandler(proxy))
                    .getAttributes(true, (Collection<String>) args[0]));

            COALESCING_METHODS.add(Table.class.getMethod("getIndex"));
            COALESCING_METHODS.add(Table.class.getMethod("getColumnSource", String.class));
            COALESCING_METHODS.add(Table.class.getMethod("getColumnSourceMap"));
            COALESCING_METHODS.add(Table.class.getMethod("getColumn", int.class));
            COALESCING_METHODS.add(Table.class.getMethod("getColumn", String.class));
            COALESCING_METHODS
                .add(Table.class.getMethod("setAttribute", String.class, Object.class));

            JOIN_METHOD_NAMES.add("join");
            JOIN_METHOD_NAMES.add("naturalJoin");
            JOIN_METHOD_NAMES.add("leftJoin");
            JOIN_METHOD_NAMES.add("exactJoin");
            AJ_METHOD_NAMES.add("aj");
            AJ_METHOD_NAMES.add("raj");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private final TableMap underlyingTableMap;
    private final boolean strictKeys;
    private final boolean allowCoalesce;
    private final boolean sanityCheckJoins;
    private Table coalesced;

    public static Table makeProxy(TableMap localTableMap, boolean strictKeys, boolean allowCoalesce,
        boolean sanityCheckJoins) {
        return (Table) Proxy.newProxyInstance(TableMapProxyHandler.class.getClassLoader(),
            new Class[] {TableMapProxy.class},
            new TableMapProxyHandler(localTableMap, strictKeys, allowCoalesce, sanityCheckJoins));
    }

    public interface TableMapProxy extends Table, TransformableTableMap {
    }

    private TableMapProxyHandler(TableMap underlyingTableMap, boolean strictKeys,
        boolean allowCoalesce, boolean sanityCheckJoins) {
        this.underlyingTableMap = underlyingTableMap;
        this.strictKeys = strictKeys;
        this.allowCoalesce = allowCoalesce;
        this.sanityCheckJoins = sanityCheckJoins;

        manage(underlyingTableMap);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final InvocationHandler handler = HIJACKED_DELEGATIONS.get(method);
        if (handler != null) {
            return handler.invoke(proxy, method, args);
        }

        if (COALESCING_METHODS.contains(method)) {
            return method.invoke(coalesce(), args);
        }

        if (method.getReturnType() != Table.class) {
            throw new UnsupportedOperationException(
                "Method is not supported by TableMapProxyHandler: " + method);
        }

        final Class<?>[] parameterTypes = method.getParameterTypes();

        int tableArgument = -1;
        boolean isTableMapProxy = false;

        for (int ii = 0; ii < parameterTypes.length; ii++) {
            if (Table.class.isAssignableFrom(parameterTypes[ii])) {
                if (tableArgument >= 0) {
                    throw new UnsupportedOperationException(
                        "Can not handle methods with multiple Table arguments!");
                }
                tableArgument = ii;
                if (args[ii] instanceof TableMapProxy) {
                    isTableMapProxy = true;
                }
            }
        }

        if (tableArgument < 0 || !isTableMapProxy) {
            return QueryPerformanceRecorder.withNugget("TableMapProxyHandler-" + method.getName(),
                () -> {
                    final TableMap resultMap = underlyingTableMap.transformTables(x -> {
                        try {
                            return (Table) method.invoke(x, args);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(
                                "Error invoking method on TableMapProxy: " + method, e);
                        }
                    });
                    return makeProxy(resultMap, strictKeys, allowCoalesce, sanityCheckJoins);
                });
        }


        final int indexOfProxy = tableArgument;

        final TableMap otherMap = ((TableMapProxy) args[tableArgument]).asTableMap();
        final List<Object> references = new ArrayList<>();
        final int finalArgument = tableArgument;

        return QueryPerformanceRecorder.withNugget("TableMapProxyHandler-" + method.getName(),
            () -> {
                if (strictKeys) {
                    final Set<Object> otherKeys =
                        new HashSet<>(Arrays.asList(otherMap.getKeySet()));
                    final Set<Object> ourKeys =
                        new HashSet<>(Arrays.asList(underlyingTableMap.getKeySet()));

                    if (!otherKeys.containsAll(ourKeys) || !ourKeys.containsAll(otherKeys)) {
                        final Set<Object> tempOther = new HashSet<>(otherKeys);
                        tempOther.removeAll(ourKeys);
                        ourKeys.removeAll(otherKeys);

                        throw new IllegalArgumentException(
                            "Strict keys is set, but key sets differ left missing="
                                + ourKeys.toString() + ", right missing=" + tempOther.toString());
                    }

                    // if we are strict and a key is added, we know something has gone wrong;
                    // because they can't be added at exactly the same time
                    final TableMap.KeyListener enforceStrictListener = key -> {
                        throw new IllegalStateException(
                            "When operating on a TableMapProxy, keys may not be added after the initial merge() call when strictKeys is set, key="
                                + key);
                    };
                    underlyingTableMap.addKeyListener(enforceStrictListener);
                    otherMap.addKeyListener(enforceStrictListener);
                    references.add(enforceStrictListener);
                }

                if (sanityCheckJoins) {
                    int keyArgument = -1;
                    boolean skipLast = false;
                    if (JOIN_METHOD_NAMES.contains(method.getName())) {
                        // we need to figure out the keys, from our first argument that is after the
                        // right table
                        keyArgument = finalArgument + 1;
                    } else if (AJ_METHOD_NAMES.contains(method.getName())) {
                        keyArgument = finalArgument + 1;
                        // we don't use the last key, because it is not used for exact match
                        skipLast = true;
                    }

                    if (keyArgument > 0) {
                        final List<MatchPair> keyColumns = new ArrayList<>();

                        final Object keyValue = args[keyArgument];
                        if (keyValue == null) {
                            throw new IllegalArgumentException(
                                "Join Keys Value is null for Join operation!");
                        }

                        final Class keyClass = keyValue.getClass();
                        if (Collection.class.isAssignableFrom(keyClass)) {
                            // we should have a collection of Strings
                            // noinspection unchecked
                            keyColumns.addAll(Arrays.asList(
                                MatchPairFactory.getExpressions((Collection<String>) (keyValue))));
                        } else if (String.class.isAssignableFrom(keyClass)) {
                            // we need to turn into MatchPairs
                            keyColumns.addAll(Arrays.asList(MatchPairFactory
                                .getExpressions(StringUtils.splitToCollection((String) keyValue))));
                        } else if (MatchPair[].class.isAssignableFrom(keyClass)) {
                            keyColumns.addAll(Arrays.asList((MatchPair[]) keyValue));
                        }

                        final String description = method.getName() + "(" + MatchPair.matchString(
                            keyColumns.toArray(new MatchPair[keyColumns.size()])) + ")";

                        if (skipLast) {
                            keyColumns.remove(keyColumns.size() - 1);
                        }

                        final Map<Object, Object> joinKeyToTableKey = new HashMap<>();

                        final String[] leftKeyNames =
                            keyColumns.stream().map(MatchPair::left).toArray(String[]::new);
                        final String[] rightKeyNames =
                            keyColumns.stream().map(MatchPair::right).toArray(String[]::new);

                        for (Object tableKey : underlyingTableMap.getKeySet()) {
                            final Table leftTable = underlyingTableMap.get(tableKey);
                            final Table rightTable = otherMap.get(tableKey);

                            final Table leftKeyTable = leftTable.selectDistinct(leftKeyNames);
                            references.add(verifyDisjointJoinKeys(description + " Left",
                                joinKeyToTableKey, tableKey, leftKeyNames, leftKeyTable));

                            final Table rightKeyTable = rightTable.selectDistinct(rightKeyNames);
                            references.add(verifyDisjointJoinKeys(description + " Right",
                                joinKeyToTableKey, tableKey, rightKeyNames, rightKeyTable));
                        }
                    }
                }

                final TableMap resultMap =
                    this.underlyingTableMap.transformTablesWithMap(otherMap, (x, y) -> {
                        final Object[] rewrittenArgs = Arrays.copyOf(args, args.length);
                        rewrittenArgs[indexOfProxy] = y;
                        try {
                            return (Table) method.invoke(x, rewrittenArgs);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            throw new RuntimeException(
                                "Error invoking method in TableMap: " + method, e);
                        }
                    });

                references.removeIf(Objects::isNull);
                references.forEach(((TableMapImpl) resultMap)::addParentReference);

                return makeProxy(resultMap, strictKeys, allowCoalesce, sanityCheckJoins);
            });
    }

    private Object verifyDisjointJoinKeys(final String description,
        final Map<Object, Object> joinKeyToTableKey, final Object tableKey, final String[] keyNames,
        final Table keyTable) {
        final JoinSanityListener listener =
            new JoinSanityListener(description, joinKeyToTableKey, tableKey, keyNames, keyTable);
        listener.checkSanity(keyTable.getIndex());

        if (((DynamicTable) keyTable).isRefreshing()) {
            ((DynamicTable) keyTable).listenForUpdates(listener);
            return listener;
        } else {
            return null;
        }
    }

    long size() {
        return underlyingTableMap.values().stream().mapToLong(Table::size).sum();
    }

    private Table coalesce() {
        if (allowCoalesce) {
            return merge();
        } else {
            throw new IllegalArgumentException("This TableMapProxyHandler may not be coalesced!");
        }
    }

    private synchronized Table merge() {
        if (!Liveness.verifyCachedObjectForReuse(coalesced)) {
            coalesced = underlyingTableMap.merge();

            final Map<String, Object> consistentAttributes =
                getAttributes(false, Collections.emptySet());
            consistentAttributes.forEach(coalesced::setAttribute);
        }
        return coalesced;
    }

    private TableMap asTableMap() {
        if (DynamicNode.isDynamicAndIsRefreshing(underlyingTableMap)) {
            LivenessScopeStack.peek().manage(underlyingTableMap);
        }
        return underlyingTableMap;
    }

    private Table asTable(boolean strictKeys, boolean allowCoalesce, boolean sanityCheckJoins) {
        return underlyingTableMap.asTable(strictKeys, allowCoalesce, sanityCheckJoins);
    }

    private TransformableTableMap.AsTableBuilder asTableBuilder() {
        return new TransformableTableMap.AsTableBuilder(underlyingTableMap)
            .allowCoalesce(allowCoalesce).sanityCheckJoin(sanityCheckJoins).strictKeys(strictKeys);
    }

    @Override
    public String toString() {
        return "TableMap.merge(" + underlyingTableMap.toString() + ')';
    }

    public TableDefinition getDefinition() {
        final Iterator<Table> it = underlyingTableMap.values().iterator();
        if (it.hasNext()) {
            return it.next().getDefinition();
        }
        // TODO: maybe the TableMap should actually know it's definition
        throw new IllegalArgumentException(
            "No tables exist in the table map, can not determine the definition.");
    }

    private boolean hasAttribute(String name) {
        final Collection<Table> underlyingTables = underlyingTableMap.values();
        if (underlyingTables.isEmpty()) {
            return false;
        }
        final Iterator<Table> it = underlyingTables.iterator();
        final boolean expected = it.next().hasAttribute(name);
        while (it.hasNext()) {
            final boolean hasAttribute = it.next().hasAttribute(name);
            if (hasAttribute != expected) {
                throw new IllegalArgumentException(
                    "Underlying tables do not have consistent presence for attribute " + name);
            }
        }
        return expected;
    }

    private Object getAttribute(String name) {
        final Collection<Table> underlyingTables = underlyingTableMap.values();
        if (underlyingTables.isEmpty()) {
            return null;
        }
        final Iterator<Table> it = underlyingTables.iterator();
        final Object expected = it.next().getAttribute(name);
        while (it.hasNext()) {
            final Object thisAttribute = it.next().getAttribute(name);
            if (!(Objects.equals(thisAttribute, expected))) {
                throw new IllegalArgumentException(
                    "Underlying tables do not have consistent value for attribute " + name);
            }
        }
        return expected;
    }

    /**
     * Get the common attributes for the merged table.
     *
     * @param assertConsistency if true, throw an IllegalArgumentException if the attributes are not
     *        consistent; otherwise return only the attributes which are consistent
     * @param excluded a set of attributes to exclude from the result
     * @return the set of common attributes for the merged table
     */
    private Map<String, Object> getAttributes(boolean assertConsistency,
        Collection<String> excluded) {
        final Collection<Table> underlyingTables = underlyingTableMap.values();
        if (underlyingTables.isEmpty()) {
            return Collections.emptyMap();
        }
        final Iterator<Table> it = underlyingTables.iterator();
        Map<String, Object> expected = it.next().getAttributes(excluded);
        boolean expectedRewritten = false;
        while (it.hasNext()) {
            final Map<String, Object> theseAttributes = it.next().getAttributes(excluded);
            if (assertConsistency) {
                // if we have no consistency, we should bomb
                if (!theseAttributes.equals(expected)) {
                    throw new IllegalArgumentException(
                        "Underlying tables do not have consistent attributes.");
                }
            } else {
                boolean expectedCopied = false;
                // make a set of consistent attributes
                for (final Iterator<Map.Entry<String, Object>> expectedIt =
                    expected.entrySet().iterator(); expectedIt.hasNext();) {
                    final Map.Entry<String, Object> expectedEntry = expectedIt.next();
                    final Object expectedValue = expectedEntry.getValue();
                    final Object thisValue = theseAttributes.get(expectedEntry.getKey());
                    if (!Objects.equals(expectedValue, thisValue)) {
                        if (expectedRewritten) {
                            expectedIt.remove();
                        } else {
                            if (!expectedCopied) {
                                expected = new ConcurrentHashMap<>(expected);
                            }
                            expected.remove(expectedEntry.getKey());
                            expectedCopied = true;
                        }
                    }
                }
                if (expectedCopied) {
                    expectedRewritten = true;
                }
                if (expected.isEmpty()) {
                    return Collections.emptyMap();
                }
            }
        }

        return expected;
    }

    @NotNull
    private Set<String> getAttributeNames() {
        final Collection<Table> underlyingTables = underlyingTableMap.values();
        if (underlyingTables.isEmpty()) {
            return Collections.emptySet();
        }
        final Iterator<Table> it = underlyingTables.iterator();
        final Set<String> expected = it.next().getAttributeNames();
        while (it.hasNext()) {
            final Set<String> theseAttributes = it.next().getAttributeNames();
            if (!theseAttributes.equals(expected)) {
                throw new IllegalArgumentException(
                    "Underlying tables do not have consistent attribute sets.");
            }
        }

        return expected;
    }

    private static class JoinSanityListener extends InstrumentedShiftAwareListenerAdapter {
        private final ColumnSource[] keyColumns;
        private final Object tableKey;
        private final String description;
        private final Map<Object, Object> joinKeyToTableKey;

        private JoinSanityListener(String description, Map<Object, Object> joinKeyToTableKey,
            Object tableKey, String[] keyNames, Table keyTable) {
            super("TableMapProxy JoinSanityListener-" + description, (DynamicTable) keyTable,
                false);
            this.description = description;
            this.joinKeyToTableKey = joinKeyToTableKey;
            keyColumns = keyTable.getColumnSources().toArray(new ColumnSource[keyNames.length]);
            this.tableKey = tableKey;
        }

        @Override
        public void onUpdate(final Update upstream) {
            checkSanity(upstream.added);
            checkSanity(upstream.modified);
        }

        private void checkSanity(Index index) {
            synchronized (joinKeyToTableKey) {
                for (final Index.Iterator it = index.iterator(); it.hasNext();) {
                    final long indexKey = it.nextLong();
                    final Object joinKey = TableTools.getKey(keyColumns, indexKey);
                    final Object existing = joinKeyToTableKey.putIfAbsent(joinKey, tableKey);
                    if (existing != null && !Objects.equals(existing, tableKey)) {
                        throw new IllegalArgumentException(description + " join key \"" + joinKey
                            + "\" exists in multiple TableMap keys, \"" + existing + "\" and \""
                            + tableKey + "\"");
                    }
                }
            }
        }
    }
}

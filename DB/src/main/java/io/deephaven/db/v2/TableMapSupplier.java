package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.base.WeakReferenceManager;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.util.annotations.ReferentialIntegrity;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

/**
 * A TableMapSupplier uses a source TableMap and applies a set of operations in the get method.
 */
public class TableMapSupplier implements TableMap {
    // The source table map is from the byExternal on the source table
    private final TableMap sourceMap;

    // This list of functions is for table map transformations
    private final List<TransformTablesFunction> functions;

    private final WeakReferenceManager<Listener> internalListeners =
        new WeakReferenceManager<>(true);

    @ReferentialIntegrity
    private final Listener internalListener = (key, table) -> {
        final MutableObject<Table> toForward = new MutableObject<>();
        internalListeners.forEachValidReference(l -> {
            if (toForward.getValue() == null) {
                toForward.setValue(applyOperations(key, table));
            }

            l.handleTableAdded(key, toForward.getValue());
        });
    };

    public TableMapSupplier(TableMap sourceMap,
        List<java.util.function.Function<Table, Table>> functions) {
        this.sourceMap = sourceMap;
        this.functions = functions.stream()
            .map(TableMapFunctionAdapter::of)
            .map(f -> new TransformTablesFunction(null, f))
            .collect(Collectors.toCollection(ArrayList::new));
        sourceMap.addListener(internalListener);
    }

    private TableMapSupplier(TableMap sourceMap, List<TransformTablesFunction> functions,
        boolean sentinel) { // sentinel forces new type-signature for constructor
        this.sourceMap = sourceMap;
        this.functions = new ArrayList<>(functions);
        sourceMap.addListener(internalListener);
    }

    @Override
    public Table get(Object key) {
        try {
            return applyOperations(key, sourceMap.get(key));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table applyOperations(Map.Entry<Object, Table> e) {
        return applyOperations(e.getKey(), e.getValue());
    }

    private Table applyOperations(Object key, Table table) {
        // Apply operations from transformations
        for (TransformTablesFunction function : functions) {
            if (table == null) {
                return null;
            }
            table = function.apply(key, table);
        }
        return table;
    }

    @Override
    public Table getWithTransform(Object key, java.util.function.Function<Table, Table> transform) {
        Table result = get(key);
        if (result == null) {
            return null;
        }

        return transform.apply(result);
    }

    @Override
    public Object[] getKeySet() {
        return sourceMap.getKeySet();
    }

    @Override
    public Collection<Entry<Object, Table>> entrySet() {
        return sourceMap.entrySet();
    }

    @Override
    public Collection<Table> values() {
        return sourceMap.entrySet().stream().map(this::applyOperations)
            .collect(Collectors.toList());
    }

    @Override
    public int size() {
        return sourceMap.size();
    }

    @Override
    public TableMap populateKeys(Object... keys) {
        return sourceMap.populateKeys(keys);
    }

    @Override
    public void addListener(Listener listener) {
        internalListeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        internalListeners.remove(listener);
    }

    @Override
    public void addKeyListener(KeyListener listener) {
        sourceMap.addKeyListener(listener);
    }

    @Override
    public void removeKeyListener(KeyListener listener) {
        sourceMap.removeKeyListener(listener);
    }

    @Override
    public TableMap flatten() {
        return transformTables(Table::flatten);
    }

    @Override
    public <R> R apply(Function.Unary<R, TableMap> function) {
        return function.call(this);
    }

    @Override
    public TableMap transformTablesWithKey(BiFunction<Object, Table, Table> function) {
        final TableMapSupplier copy = new TableMapSupplier(sourceMap, functions, true);
        copy.functions.add(new TransformTablesFunction(function));
        return copy;
    }

    @Override
    public TableMap transformTablesWithKey(TableDefinition returnDefinition,
        BiFunction<Object, Table, Table> function) {
        final TableMapSupplier copy = new TableMapSupplier(sourceMap, functions, true);
        copy.functions.add(new TransformTablesFunction(returnDefinition, function));
        return copy;
    }

    @Override
    public TableMap transformTablesWithMap(TableMap otherMap,
        BiFunction<Table, Table, Table> function) {
        throw new UnsupportedOperationException(
            "TableSupplierMap does not support transformTablesWithMap");
    }

    @Override
    public boolean tryManage(@NotNull LivenessReferent referent) {
        return sourceMap.tryManage(referent);
    }

    @Override
    public boolean tryRetainReference() {
        return sourceMap.tryRetainReference();
    }

    @Override
    public void dropReference() {
        sourceMap.dropReference();
    }

    @Override
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        return sourceMap.getWeakReference();
    }

    @Override
    public Table merge() {
        // note: this is different than the previous logic - we are doing are operations on the
        // inner tables first
        return applyFunctionsToSourceMap().merge();
    }

    @Override
    public Table asTable(boolean strictKeys, boolean allowCoalesce, boolean sanityCheckJoins) {
        // note: this is different than the previous logic - we are doing are operations on the
        // inner tables first
        return applyFunctionsToSourceMap().asTable(strictKeys, allowCoalesce, sanityCheckJoins);
    }

    private TableMap applyFunctionsToSourceMap() {
        TableMap tm = sourceMap;
        for (TransformTablesFunction function : functions) {
            tm = function.apply(tm);
        }
        return tm;
    }
}

package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectHashMap;
import org.jetbrains.annotations.NotNull;

/**
 * Partial TableDataService implementation for use by TableDataService implementations.
 */
public abstract class AbstractTableDataService implements TableDataService {

    private final String name;
    private final KeyedObjectHashMap<TableKey, TableLocationProvider> tableLocationProviders = new KeyedObjectHashMap<>(TableKey.getKeyedObjectKey());

    protected AbstractTableDataService(String name) {
        this.name = name;
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableDataService implementation
    //------------------------------------------------------------------------------------------------------------------

    // TODO: Consider overriding getTableLocationProvider(ns,tn,tt) to use a ThreadLocal<TableLookupKey.Reusable>

    @Override
    public @NotNull final TableLocationProvider getTableLocationProvider(@NotNull final TableKey tableKey) {
        return tableLocationProviders.putIfAbsent(tableKey, this::makeTableLocationProvider);
    }

    @Override
    public void reset() {
        tableLocationProviders.clear();
    }

    @Override
    public void reset(TableKey key) {
        tableLocationProviders.remove(key);
    }

    protected abstract @NotNull TableLocationProvider makeTableLocationProvider(@NotNull TableKey tableKey);

    @Override
    public String getName() {
        return name;
    }
}

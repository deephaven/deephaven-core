package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Partial {@link TableDataService} implementation.
 */
public abstract class AbstractTableDataService<TKT extends TableKey, TLKT extends TableLocationKey> implements TableDataService<TKT, TLKT> {

    private final String name;

    private final KeyedObjectHashMap<TKT, TableLocationProvider<TKT, TLKT>> tableLocationProviders;

    /**
     * Construct an AbstractTableDataService.
     *
     * @param name         Optional service name
     */
    protected AbstractTableDataService(@Nullable final String name) {
        this.name = name;
        this.tableLocationProviders = new KeyedObjectHashMap<>(new ProviderKeyDefinition<>());
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableDataService implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final TableLocationProvider<TKT, TLKT> getTableLocationProvider(@NotNull final TKT tableKey) {
        return tableLocationProviders.putIfAbsent(tableKey, this::makeTableLocationProvider);
    }

    @Override
    public void reset() {
        tableLocationProviders.clear();
    }

    @Override
    public void reset(@NotNull final TKT key) {
        tableLocationProviders.remove(key);
    }

    protected abstract @NotNull TableLocationProvider<TKT, TLKT> makeTableLocationProvider(@NotNull TKT tableKey);

    @Override
    public String getName() {
        return name;
    }

    //------------------------------------------------------------------------------------------------------------------
    // Default key definition implementation
    //------------------------------------------------------------------------------------------------------------------

    private static final class ProviderKeyDefinition<TKT extends TableKey, TLKT extends TableLocationKey> extends KeyedObjectKey.Basic<TKT, TableLocationProvider<TKT, TLKT>> {

        @Override
        public TKT getKey(@NotNull final TableLocationProvider<TKT, TLKT> tableLocationProvider) {
            return tableLocationProvider.getKey();
        }
    }
}

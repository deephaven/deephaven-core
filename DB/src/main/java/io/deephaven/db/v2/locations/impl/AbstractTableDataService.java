package io.deephaven.db.v2.locations.impl;

import io.deephaven.db.v2.locations.TableDataService;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Partial {@link TableDataService} implementation.
 */
public abstract class AbstractTableDataService implements TableDataService, NamedImplementation {

    private final String name;

    private final KeyedObjectHashMap<TableKey, TableLocationProvider> tableLocationProviders = new KeyedObjectHashMap<>(ProviderKeyDefinition.INSTANCE);

    /**
     * Construct an AbstractTableDataService.
     *
     * @param name Optional service name
     */
    protected AbstractTableDataService(@Nullable final String name) {
        this.name = name;
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableDataService implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final TableLocationProvider getTableLocationProvider(@NotNull final TableKey tableKey) {
        return tableLocationProviders.putIfAbsent(tableKey, this::makeTableLocationProvider);
    }

    @Override
    public void reset() {
        tableLocationProviders.clear();
    }

    @Override
    public void reset(@NotNull final TableKey tableKey) {
        tableLocationProviders.remove(tableKey);
    }

    @NotNull
    protected abstract TableLocationProvider makeTableLocationProvider(@NotNull TableKey tableKey);

    @Override
    public String getName() {
        return name;
    }

    /**
     * Key definition for {@link TableLocationProvider} lookup by {@link TableKey}.
     */
    private static final class ProviderKeyDefinition extends KeyedObjectKey.Basic<TableKey, TableLocationProvider> {

        private static final KeyedObjectKey<TableKey, TableLocationProvider> INSTANCE = new ProviderKeyDefinition();

        private ProviderKeyDefinition() {
        }

        @Override
        public TableKey getKey(@NotNull final TableLocationProvider tableLocationProvider) {
            return tableLocationProvider.getKey();
        }
    }
}

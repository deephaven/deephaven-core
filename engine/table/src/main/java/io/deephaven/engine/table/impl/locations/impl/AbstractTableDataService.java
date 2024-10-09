//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.TableDataService;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
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

    private final KeyedObjectHashMap<TableKey, TableLocationProvider> tableLocationProviders =
            new KeyedObjectHashMap<>(ProviderKeyDefinition.INSTANCE);

    /**
     * Construct an AbstractTableDataService.
     *
     * @param name Optional service name
     */
    protected AbstractTableDataService(@Nullable final String name) {
        this.name = name;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableDataService implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final TableLocationProvider getTableLocationProvider(@NotNull final TableKey tableKey) {
        return tableLocationProviders.putIfAbsent(tableKey, this::makeTableLocationProvider);
    }

    @Override
    @Nullable
    public TableLocationProvider getRawTableLocationProvider(@NotNull TableKey tableKey,
            @NotNull TableLocationKey tableLocationKey) {
        final TableLocationProvider tableLocationProvider = tableLocationProviders.get(tableKey);
        if (tableLocationProvider == null || !tableLocationProvider.hasTableLocationKey(tableLocationKey)) {
            return null;
        }

        return tableLocationProvider;
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

        private ProviderKeyDefinition() {}

        @Override
        public TableKey getKey(@NotNull final TableLocationProvider tableLocationProvider) {
            return tableLocationProvider.getKey();
        }
    }
}

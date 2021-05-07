package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link TableLocationProvider} that provides access to exactly one, previously-known {@link TableLocation}.
 */
public class SingleTableLocationProvider implements TableLocationProvider {

    private final TableLocation tableLocation;

    /**
     * @param tableLocation The only table location that this provider will ever provide
     */
    public SingleTableLocationProvider(@NotNull final TableLocation tableLocation) {
        this.tableLocation = tableLocation;
    }

    @Override
    public boolean supportsSubscriptions() {
        return false;
    }

    @Override
    public void subscribe(@NotNull Listener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe(@NotNull Listener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh() {
    }

    @Override
    public TableLocationProvider ensureInitialized() {
        return this;
    }

    @NotNull
    @Override
    public Collection<TableLocation> getTableLocations() {
        return Collections.singleton(tableLocation);
    }

    @Nullable
    @Override
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        return TableLocationKey.areEqual(tableLocationKey, tableLocation) ? tableLocation : null;
    }

    @NotNull
    @Override
    public CharSequence getNamespace() {
        return tableLocation.getTableKey().getNamespace();
    }

    @NotNull
    @Override
    public CharSequence getTableName() {
        return tableLocation.getTableKey().getTableName();
    }

    @NotNull
    @Override
    public TableType getTableType() {
        return tableLocation.getTableKey().getTableType();
    }
}

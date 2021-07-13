package io.deephaven.db.v2.locations;

import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link TableLocationProvider} that provides access to a fixed set of previously-known {@link TableLocation}s.
 */
public class FixedTableLocationProvider implements TableLocationProvider {
    private static final Logger log = ProcessEnvironment.getDefaultLog(FixedTableLocationProvider.class);
    private final TableKey tableKey;
    private final List<TableLocation> locations;
    private final String serviceName;

    public FixedTableLocationProvider(@NotNull final TableKey tableKey, @NotNull String serviceName, @NotNull final Collection<TableLocation> locations) {
        this.tableKey = tableKey;
        this.serviceName  = serviceName;
        this.locations = Collections.unmodifiableList(new ArrayList<>(locations));
    }

    @Override
    public boolean supportsSubscriptions() {
        return false;
    }

    @Override
    public void subscribe(@NotNull TableLocationProvider.Listener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe(@NotNull TableLocationProvider.Listener listener) {
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
    public Collection<TableLocation> getTableLocationKeys() {
        return locations;
    }

    @Nullable
    @Override
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        return locations.stream().filter(location -> TableLocationKey.areEqual(location, tableLocationKey)).findFirst().orElse(null);
    }

    @NotNull
    @Override
    public CharSequence getNamespace() {
        return tableKey.getNamespace();
    }

    @NotNull
    @Override
    public CharSequence getTableName() {
        return tableKey.getTableName();
    }

    @NotNull
    @Override
    public TableType getTableType() {
        return tableKey.getTableType();
    }

    @Override
    public String getImplementationName() {
        return "FixedTableLocationProvider";
    }
}

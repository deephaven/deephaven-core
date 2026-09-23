//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.ImmutableTableKey;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A {@link TableLocationProvider} that provides no locations, for a table that is known to have none visible.
 */
public class NullTableLocationProvider implements TableLocationProvider {

    private final ImmutableTableKey tableKey;

    /**
     * Constructs a provider with no locations for the given table.
     *
     * @param tableKey the key for the table with no locations
     */
    public NullTableLocationProvider(@NotNull final TableKey tableKey) {
        this.tableKey = tableKey.makeImmutable();
    }

    @Override
    public ImmutableTableKey getKey() {
        return tableKey;
    }

    @Override
    @NotNull
    public TableUpdateMode getUpdateMode() {
        return TableUpdateMode.STATIC;
    }

    @Override
    @NotNull
    public TableUpdateMode getLocationUpdateMode() {
        return TableUpdateMode.STATIC;
    }

    @Override
    public boolean supportsSubscriptions() {
        return false;
    }

    @Override
    public void subscribe(@NotNull final Listener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe(@NotNull final Listener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh() {}

    @Override
    public TableLocationProvider ensureInitialized() {
        return this;
    }

    @Override
    @NotNull
    public Collection<ImmutableTableLocationKey> getTableLocationKeys() {
        return List.of();
    }

    @Override
    public void getTableLocationKeys(
            final Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
            final Predicate<ImmutableTableLocationKey> filter) {}

    @Override
    public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
        return false;
    }

    @Override
    @Nullable
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        return null;
    }

    @Override
    public String toString() {
        return getImplementationName() + '{' + tableKey + '}';
    }
}

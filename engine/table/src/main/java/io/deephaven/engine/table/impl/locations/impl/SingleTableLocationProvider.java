//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * A {@link TableLocationProvider} that provides access to exactly one, previously-known {@link TableLocation}.
 */
public final class SingleTableLocationProvider implements TableLocationProvider {

    private static final String IMPLEMENTATION_NAME = SingleTableLocationProvider.class.getSimpleName();

    private final TableLocation tableLocation;

    /**
     * @param tableLocation The only table location that this provider will ever provide
     */
    public SingleTableLocationProvider(@NotNull final TableLocation tableLocation) {
        this.tableLocation = tableLocation;
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public ImmutableTableKey getKey() {
        return tableLocation.getTableKey();
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
    public @NotNull Collection<TrackedTableLocationKey> getTableLocationKeys(Predicate<TableLocationKey> filter) {
        // TODO: should TableLocation#getKey() be a TrackedTableLocationKey? This is getting complicated.
        return filter.test(tableLocation.getKey())
                ? Collections.singleton(tableLocation.getKey())
                : List.of();
    }

    @Override
    public boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
        return tableLocation.getKey().equals(tableLocationKey);
    }

    @Nullable
    @Override
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        return hasTableLocationKey(tableLocationKey) ? tableLocation : null;
    }
}

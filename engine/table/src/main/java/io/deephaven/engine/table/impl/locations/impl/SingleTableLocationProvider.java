//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A {@link TableLocationProvider} that provides access to exactly one, previously-known {@link TableLocation}.
 */
public final class SingleTableLocationProvider implements TableLocationProvider {
    private static final String IMPLEMENTATION_NAME = SingleTableLocationProvider.class.getSimpleName();

    private static class TrackedKeySupplier extends ReferenceCountedLivenessNode
            implements LiveSupplier<ImmutableTableLocationKey> {
        final ImmutableTableLocationKey key;

        protected TrackedKeySupplier(final ImmutableTableLocationKey key) {
            super(false);
            this.key = key;
        }

        @Override
        public ImmutableTableLocationKey get() {
            return key;
        }
    }

    private final TrackedKeySupplier immutableKeySupplier;
    private final TableLocation tableLocation;

    /**
     * @param tableLocation The only table location that this provider will ever provide
     */
    public SingleTableLocationProvider(@NotNull final TableLocation tableLocation) {
        this.tableLocation = tableLocation;
        // TODO: it seems like we should manage this, but SingleTableLocationProvider isn't a LivenessManager.
        immutableKeySupplier = new TrackedKeySupplier(tableLocation.getKey());
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
    public void getTableLocationKeys(
            final Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
            final Predicate<ImmutableTableLocationKey> filter) {
        if (filter.test(immutableKeySupplier.get())) {
            consumer.accept(immutableKeySupplier);
        }
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

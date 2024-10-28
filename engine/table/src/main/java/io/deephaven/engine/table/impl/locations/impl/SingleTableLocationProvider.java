//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.locations.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A {@link TableLocationProvider} that provides access to exactly one, previously-known {@link TableLocation}. In
 * contrast to {@link AbstractTableLocationProvider}, this class does not manage the liveness of the table location.
 * Managment must be done externally (as in {@link io.deephaven.engine.table.impl.SourcePartitionedTable}).
 */
public final class SingleTableLocationProvider implements TableLocationProvider {
    private static final String IMPLEMENTATION_NAME = SingleTableLocationProvider.class.getSimpleName();

    private static class TrackedKeySupplier implements LiveSupplier<ImmutableTableLocationKey>, LivenessReferent {
        final ImmutableTableLocationKey key;

        protected TrackedKeySupplier(final ImmutableTableLocationKey key) {
            this.key = key;
        }

        @Override
        public ImmutableTableLocationKey get() {
            return key;
        }

        @Override
        public boolean tryRetainReference() {
            return true;
        }

        @Override
        public void dropReference() {}

        @Override
        public WeakReference<? extends LivenessReferent> getWeakReference() {
            return new WeakReference<>(this);
        }
    }

    private final TrackedKeySupplier immutableKeySupplier;
    private final TableLocation tableLocation;
    private final TableUpdateMode locationUpdateMode;

    /**
     * @param tableLocation The only table location that this provider will ever provide
     */
    public SingleTableLocationProvider(
            @NotNull final TableLocation tableLocation,
            final TableUpdateMode locationUpdateMode) {
        this.tableLocation = tableLocation;
        immutableKeySupplier = new TrackedKeySupplier(tableLocation.getKey());
        this.locationUpdateMode = locationUpdateMode;
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

    @Override
    @NotNull
    public TableUpdateMode getUpdateMode() {
        // No additions or removals are possible from this provider, it exists to serve
        return TableUpdateMode.STATIC;
    }

    @Override
    @NotNull
    public TableUpdateMode getLocationUpdateMode() {
        return locationUpdateMode;
    }
}

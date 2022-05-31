package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Interface for {@link TableLocationKey} discovery with delivery to a callback.
 */
@FunctionalInterface
public interface TableLocationKeyFinder<TLK extends TableLocationKey> {

    /**
     * Find {@link TableLocationKey keys} and deliver them to the {@code locationKeyObserver}.
     *
     * @param locationKeyObserver Per-key callback
     */
    void findKeys(@NotNull Consumer<TLK> locationKeyObserver);
}

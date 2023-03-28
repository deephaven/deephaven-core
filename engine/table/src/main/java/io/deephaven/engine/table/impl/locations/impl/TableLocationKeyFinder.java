/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
     * May create a new TableLocationKeyFinder with additional safety checks.
     *
     * @param finder The finder
     * @return The finder, potentially with additional safety checks
     * @param <TLK> The {@link TableLocationKey} type
     */
    static <TLK extends TableLocationKey> TableLocationKeyFinder<TLK> safetyCheck(
            @NotNull TableLocationKeyFinder<TLK> finder) {
        return new TableLocationKeySafetyImpl<>(finder);
    }

    /**
     * Find {@link TableLocationKey keys} and deliver them to the {@code locationKeyObserver}.
     *
     * @param locationKeyObserver Per-key callback
     */
    void findKeys(@NotNull Consumer<TLK> locationKeyObserver);
}

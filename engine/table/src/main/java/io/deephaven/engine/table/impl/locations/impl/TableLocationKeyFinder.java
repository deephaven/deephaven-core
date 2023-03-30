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
     * Create a new TableLocationKeyFinder with additional safety checks. The additional safety checks verify that the
     * following are {@code true}:
     * <ul>
     * <li>All returned {@link TableLocationKey#getPartitionKeys()} are equal and in the same order across all calls to
     * {@link TableLocationKeyFinder#findKeys(Consumer)}</li>
     * <li>Duplicates aren't returned during a single call to {@link TableLocationKeyFinder#findKeys(Consumer)}</li>
     * <li>Optionally: once a key is returned from {@link TableLocationKeyFinder#findKeys(Consumer)}, it is always
     * returned on subsequent calls</li>
     * </ul>
     *
     * Additional safety checks may be added in the future.
     *
     * @param finder The finder
     * @param verifyNoneRemoved If the safety check should include the verification that keys are not removed
     * @return The finder, potentially with additional safety checks
     * @param <TLK> The {@link TableLocationKey} type
     */
    static <TLK extends TableLocationKey> TableLocationKeyFinder<TLK> safetyCheck(
            @NotNull TableLocationKeyFinder<TLK> finder, boolean verifyNoneRemoved) {
        return new TableLocationKeySafetyImpl<>(finder, verifyNoneRemoved);
    }

    /**
     * Find {@link TableLocationKey keys} and deliver them to the {@code locationKeyObserver}.
     *
     * @param locationKeyObserver Per-key callback
     */
    void findKeys(@NotNull Consumer<TLK> locationKeyObserver);
}

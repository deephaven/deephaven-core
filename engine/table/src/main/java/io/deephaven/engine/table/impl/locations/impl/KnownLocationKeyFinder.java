/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * {@link TableLocationKeyFinder Location finder} that delivers a known set of {@link ImmutableTableLocationKey keys}.
 */
public final class KnownLocationKeyFinder<TLK extends ImmutableTableLocationKey>
        implements TableLocationKeyFinder<TLK> {

    /**
     * Creates a copy of the keys from {@code finder}. If {@code comparator} is not {@code null}, the keys will be
     * sorted according to that {@code comparator}.
     *
     * @param finder the finder
     * @param comparator the comparator
     * @return the known location finder
     * @param <TLK> the table location key type
     */
    public static <TLK extends ImmutableTableLocationKey> KnownLocationKeyFinder<TLK> copyFrom(
            TableLocationKeyFinder<TLK> finder, Comparator<TLK> comparator) {
        final RecordingLocationKeyFinder<TLK> recordingFinder = new RecordingLocationKeyFinder<>();
        finder.findKeys(recordingFinder);
        final List<TLK> mutableKeys = recordingFinder.getRecordedKeys();
        if (comparator != null) {
            mutableKeys.sort(comparator);
        }
        final String comparatorString = comparator == null
                ? null
                : Comparator.naturalOrder().equals(comparator)
                        ? "Comparator.naturalOrder()"
                        : comparator.toString();
        final String toString =
                String.format("%s[%s, %s]", KnownLocationKeyFinder.class.getSimpleName(), finder, comparatorString);
        return new KnownLocationKeyFinder<>(mutableKeys, toString);
    }

    private final List<TLK> knownKeys;
    private final String toString;

    @SafeVarargs
    public KnownLocationKeyFinder(@NotNull final TLK... knownKeys) {
        this(Arrays.asList(knownKeys));
    }

    public KnownLocationKeyFinder(List<TLK> knownKeys) {
        this(knownKeys, null);
    }

    public KnownLocationKeyFinder(List<TLK> knownKeys, String toString) {
        this.knownKeys = List.copyOf(knownKeys);
        this.toString = toString;
    }

    /**
     * @return An unmodifiable list of recorded (immutable) keys
     */
    public List<TLK> getKnownKeys() {
        return knownKeys;
    }

    public Optional<TLK> getFirstKey() {
        return knownKeys.isEmpty() ? Optional.empty() : Optional.of(knownKeys.get(0));
    }

    public Optional<TLK> getLastKey() {
        return knownKeys.isEmpty() ? Optional.empty() : Optional.of(knownKeys.get(knownKeys.size() - 1));
    }

    @Override
    public void findKeys(@NotNull Consumer<TLK> locationKeyObserver) {
        knownKeys.forEach(locationKeyObserver);
    }

    @Override
    public String toString() {
        return toString == null ? super.toString() : toString;
    }
}

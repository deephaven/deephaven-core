package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * {@link TableLocationKeyFinder Location finder} that delivers a known set of {@link ImmutableTableLocationKey keys}.
 */
public final class KnownLocationKeyFinder<TLK extends ImmutableTableLocationKey>
        implements TableLocationKeyFinder<TLK> {

    private final List<TLK> knownKeys;

    @SafeVarargs
    public KnownLocationKeyFinder(@NotNull final TLK... knownKeys) {
        Require.elementsNeqNull(knownKeys, "knownKeys");
        this.knownKeys = knownKeys.length == 0
                ? Collections.emptyList()
                : Collections.unmodifiableList(
                        knownKeys.length == 1
                                ? Collections.singletonList(knownKeys[0])
                                : Arrays.asList(knownKeys));
    }

    /**
     * @return An unmodifiable list of recorded (immutable) keys
     */
    public List<TLK> getKnownKeys() {
        return knownKeys;
    }

    @Override
    public void findKeys(@NotNull Consumer<TLK> locationKeyObserver) {
        knownKeys.forEach(locationKeyObserver);
    }
}

package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * {@link TableLocationKeyFinder Location finder} that will record and expose the output of another for subsequent
 * delivery to an observer.
 */
public final class RecordingLocationKeyFinder<TLK extends ImmutableTableLocationKey>
        implements TableLocationKeyFinder<TLK>, Consumer<TLK> {

    private final List<TLK> recordedKeys = new ArrayList<>();

    @Override
    public void accept(@NotNull final TLK tableLocationKey) {
        recordedKeys.add(tableLocationKey);
    }

    /**
     * @return The (mutable) list of recorded (immutable) keys
     */
    public List<TLK> getRecordedKeys() {
        return recordedKeys;
    }

    @Override
    public void findKeys(@NotNull Consumer<TLK> locationKeyObserver) {
        recordedKeys.forEach(locationKeyObserver);
    }
}

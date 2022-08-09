package io.deephaven.engine.table.impl.by;

import org.jetbrains.annotations.NotNull;

import java.util.function.LongConsumer;

/**
 * Re-usable support for recording reincarnated and emptied states in incremental aggregation processing.
 */
public class StateChangeRecorder {

    private LongConsumer reincarnatedDestinationCallback;
    private LongConsumer emptiedDestinationCallback;

    /**
     * Set {@link LongConsumer callbacks} that should be used to record destinations that have transitioned from empty
     * to non-empty ({@code reincarnatedDestinationCallback}) or non-empty to empty
     * ({@code emptiedDestinationCallback}).
     *
     * @param reincarnatedDestinationCallback Consumer for destinations that have gone from empty to non-empty
     * @param emptiedDestinationCallback Consumer for destinations that have gone from non-empty to empty
     */
    void recordStateChanges(
            @NotNull final LongConsumer reincarnatedDestinationCallback,
            @NotNull final LongConsumer emptiedDestinationCallback) {
        this.reincarnatedDestinationCallback = reincarnatedDestinationCallback;
        this.emptiedDestinationCallback = emptiedDestinationCallback;
    }

    void resetStateChangeRecording() {
        reincarnatedDestinationCallback = null;
        emptiedDestinationCallback = null;
    }

    protected void onReincarnated(final long destination) {
        if (reincarnatedDestinationCallback != null) {
            reincarnatedDestinationCallback.accept(destination);
        }
    }

    protected void onEmptied(final long destination) {
        if (emptiedDestinationCallback != null) {
            emptiedDestinationCallback.accept(destination);
        }
    }
}

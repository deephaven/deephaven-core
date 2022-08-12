package io.deephaven.engine.table.impl.by;

import java.util.function.LongConsumer;

/**
 * Re-usable support for recording reincarnated and emptied states in incremental aggregation processing.
 */
class BasicStateChangeRecorder implements StateChangeRecorder {

    private LongConsumer reincarnatedDestinationCallback;
    private LongConsumer emptiedDestinationCallback;

    @Override
    public final void startRecording(
            final LongConsumer reincarnatedDestinationCallback,
            final LongConsumer emptiedDestinationCallback) {
        this.reincarnatedDestinationCallback = reincarnatedDestinationCallback;
        this.emptiedDestinationCallback = emptiedDestinationCallback;
    }

    @Override
    public final void finishRecording() {
        reincarnatedDestinationCallback = null;
        emptiedDestinationCallback = null;
    }

    /**
     * Record a reincarnated {@code destination}.
     *
     * @param destination The destination slot that has been reincarnated
     */
    final void onReincarnated(final long destination) {
        if (reincarnatedDestinationCallback != null) {
            reincarnatedDestinationCallback.accept(destination);
        }
    }

    /**
     * Record an emptied {@code destination}.
     *
     * @param destination The destination slot that has been emptied
     */
    final void onEmptied(final long destination) {
        if (emptiedDestinationCallback != null) {
            emptiedDestinationCallback.accept(destination);
        }
    }
}

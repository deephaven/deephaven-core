package io.deephaven.engine.table.impl.by;

import java.util.function.LongConsumer;

/**
 * Interface for recording reincarnated and emptied states in incremental aggregation processing.
 */
interface StateChangeRecorder {

    /**
     * Set {@link LongConsumer callbacks} that should be used to record destinations that have transitioned from empty
     * to non-empty ({@code reincarnatedDestinationCallback}) or non-empty to empty
     * ({@code emptiedDestinationCallback}).
     *
     * @param reincarnatedDestinationCallback Consumer for destinations that have gone from empty to non-empty
     * @param emptiedDestinationCallback Consumer for destinations that have gone from non-empty to empty
     */
    void startRecording(LongConsumer reincarnatedDestinationCallback, LongConsumer emptiedDestinationCallback);

    /**
     * Remove callbacks and stop state change recording.
     */
    void finishRecording();
}

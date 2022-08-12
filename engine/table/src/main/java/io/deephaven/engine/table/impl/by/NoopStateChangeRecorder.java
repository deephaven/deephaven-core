package io.deephaven.engine.table.impl.by;

import java.util.function.LongConsumer;

/**
 * Re-usable support for <em>not</em> recording reincarnated and emptied states in incremental aggregation processing,
 * for operators that never process any removes.
 */
class NoopStateChangeRecorder implements StateChangeRecorder {

    @Override
    public final void startRecording(
            final LongConsumer reincarnatedDestinationCallback,
            final LongConsumer emptiedDestinationCallback) {}

    @Override
    public final void finishRecording() {}
}

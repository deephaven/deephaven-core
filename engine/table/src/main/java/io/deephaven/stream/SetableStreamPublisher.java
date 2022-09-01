package io.deephaven.stream;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class SetableStreamPublisher implements StreamPublisher {
    private Runnable flushDelegate;
    private StreamConsumer consumer;

    public StreamConsumer consumer() {
        return Objects.requireNonNull(consumer);
    }

    public void setFlushDelegate(Runnable flushDelegate) {
        if (this.flushDelegate != null) {
            throw new IllegalStateException("Can only set flush delegate once");
        }
        this.flushDelegate = flushDelegate;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void flush() {
        if (flushDelegate != null) {
            flushDelegate.run();
        }
    }
}

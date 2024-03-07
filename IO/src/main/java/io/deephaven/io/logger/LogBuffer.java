//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.logger;

import io.deephaven.base.RingBuffer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CopyOnWriteArraySet;

public class LogBuffer implements LogBufferRecordListener {

    public static final int DEFAULT_HISTORY_SIZE = (1 << 10) - 1;

    protected final RingBuffer<LogBufferRecord> history;

    private final CopyOnWriteArraySet<LogBufferRecordListener> listeners = new CopyOnWriteArraySet<>();

    public LogBuffer(final int historySize) {
        this.history = new RingBuffer<>(historySize);
    }

    public LogBuffer() {
        this(DEFAULT_HISTORY_SIZE);
    }

    public int capacity() {
        return history.capacity();
    }

    public synchronized void clear() {
        history.clear();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LogBufferRecordListener impl
    // -----------------------------------------------------------------------------------------------------------------

    public synchronized LogBufferRecord recordInternal(@NotNull final LogBufferRecord record) {
        // A listener may choose to unsubscribe while consuming a record, so this needs to be a collection that supports
        // concurrent removals.
        for (final LogBufferRecordListener listener : listeners) {
            listener.record(record);
        }
        return history.addOverwrite(record);
    }

    @Override
    public synchronized void record(@NotNull final LogBufferRecord record) {
        recordInternal(record);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Subscription Management
    // -----------------------------------------------------------------------------------------------------------------

    public synchronized void subscribe(final LogBufferRecordListener listener) {
        listeners.add(listener);
        history.forEach(listener::record);
    }

    public synchronized void unsubscribe(final LogBufferRecordListener listener) {
        listeners.remove(listener);
    }

    public synchronized int subscriberCount() {
        return listeners.size();
    }
}

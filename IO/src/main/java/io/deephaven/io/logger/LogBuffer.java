/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.RingBuffer;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class LogBuffer implements LogBufferRecordListener {

    public static final int DEFAULT_HISTORY_SIZE = (1 << 10) - 1;

    protected final RingBuffer<LogBufferRecord> history;

    private final List<LogBufferRecordListener> listeners = new ArrayList<>();

    public LogBuffer(final int historySize) {
        this.history = new RingBuffer<>(historySize);
    }

    public LogBuffer() {
        this(DEFAULT_HISTORY_SIZE);
    }

    public synchronized void clear() {
        history.clear();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LogBufferRecordListener impl
    // -----------------------------------------------------------------------------------------------------------------

    public synchronized LogBufferRecord recordInternal(@NotNull final LogBufferRecord record) {
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
        for (final RingBuffer<LogBufferRecord>.Iterator ri = history.iterator(); ri.hasNext();) {
            listener.record(ri.next());
        }
    }

    public synchronized void unsubscribe(final LogBufferRecordListener listener) {
        listeners.remove(listener);
    }

    public synchronized int subscriberCount() {
        return listeners.size();
    }
}

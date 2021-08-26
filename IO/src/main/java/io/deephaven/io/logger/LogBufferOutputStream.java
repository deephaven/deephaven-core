package io.deephaven.io.logger;

import io.deephaven.io.log.LogLevel;
import io.deephaven.io.streams.SimpleByteBufferSink;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

class LogBufferOutputStream extends OutputStream {

    private final LogBuffer sink;
    private final LogLevel level;
    private final int initialBufferSize;
    private final int maxBufferSize;

    private LogBufferRecord next;
    private SimpleByteBufferSink buffer;

    public LogBufferOutputStream(LogBuffer sink, LogLevel level, int initialBufferSize, int maxBufferSize) {
        this.sink = Objects.requireNonNull(sink);
        this.level = Objects.requireNonNull(level);
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
        primeNext(null);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        buffer.ensureSpace(1).put((byte) b);
        if ((byte) b == '\n' || buffer.getBuffer().position() >= maxBufferSize) {
            record();
        }
    }

    private void record() {
        final ByteBuffer out = buffer.getBuffer();
        out.flip();

        next.setLevel(level);
        next.setData(out);
        // TODO (core#91): Use injectable TimeProvider for LogBufferOutputStream
        next.setTimestampMicros(System.currentTimeMillis() * 1000);

        LogBufferRecord removed = sink.recordInternal(next);
        primeNext(removed);
    }

    private void primeNext(LogBufferRecord record) {
        if (record != null) {
            // re-use the removed record and buffer.
            next = record;
            record.getData().clear();
            buffer = new SimpleByteBufferSink(record.getData());
        } else {
            next = new LogBufferRecord();
            buffer = new SimpleByteBufferSink(ByteBuffer.allocate(initialBufferSize));
        }
    }
}

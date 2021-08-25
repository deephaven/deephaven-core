package io.deephaven.io.logger;

import io.deephaven.base.system.StandardStreamReceiver;
import io.deephaven.io.log.LogLevel;

import java.io.OutputStream;
import java.util.Objects;
import java.util.Optional;

public class StreamToLogBuffer implements StandardStreamReceiver {

    private final LogBuffer logBuffer;
    private final boolean receiveOut;
    private final boolean receiveErr;
    private final int initialBufferSize;
    private final int maxBufferSize;

    public StreamToLogBuffer(LogBuffer logBuffer, boolean receiveOut, boolean receiveErr,
        int initialBufferSize, int maxBufferSize) {
        this.logBuffer = Objects.requireNonNull(logBuffer);
        this.receiveOut = receiveOut;
        this.receiveErr = receiveErr;
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Optional<OutputStream> receiveOut() {
        if (!receiveOut) {
            return Optional.empty();
        }
        return Optional.of(new LogBufferOutputStream(logBuffer, LogLevel.STDOUT, initialBufferSize,
            maxBufferSize));
    }

    @Override
    public Optional<OutputStream> receiveErr() {
        if (!receiveErr) {
            return Optional.empty();
        }
        return Optional.of(new LogBufferOutputStream(logBuffer, LogLevel.STDERR, initialBufferSize,
            maxBufferSize));
    }
}

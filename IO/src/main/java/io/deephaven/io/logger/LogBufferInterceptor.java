/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.streams.ByteBufferOutputStream;
import io.deephaven.io.streams.SimpleByteBufferSink;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;

public class LogBufferInterceptor extends LogBuffer implements LogSink.Interceptor<LogEntry> {

    public static final int RECORD_INITIAL_DATA_SIZE = 256;

    private LogBufferRecord next;

    public LogBufferInterceptor(final int historySize) {
        super(historySize);
    }

    public LogBufferInterceptor() {
        super();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LogSink.Interceptor impl
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void element(@NotNull final LogEntry e, @NotNull final LogOutput output) throws IOException {
        if (e.getLevel() instanceof LogLevel.MailLevel) {
            // We don't support MAILER log lines.
            return;
        }
        // TODO: Lighter weight synchronization? Off-thread the StreamLoggerImpl's sink?
        synchronized (this) {
            if (next == null) {
                next = new LogBufferRecord();
                next.setData(ByteBuffer.allocate(RECORD_INITIAL_DATA_SIZE));
            }
            next.setTimestampMicros(e.getTimestampMicros());
            next.setLevel(e.getLevel());
            next.getData().clear();
            final SimpleByteBufferSink sink = new SimpleByteBufferSink(next.getData());
            final ByteBufferOutputStream stream = new ByteBufferOutputStream(next.getData(), sink);
            try {
                for (int bi = 0; bi < output.getBufferCount(); ++bi) {
                    final ByteBuffer outputBuffer = output.getBuffer(bi);
                    final int initialPosition = outputBuffer.position();
                    final int initialLimit = outputBuffer.limit();
                    stream.write(output.getBuffer(bi));
                    outputBuffer.limit(initialLimit).position(initialPosition);
                }
                stream.close();
            } catch (IOException x) {
                throw new IOException("Unexpected IOException while formatting LogBuffer Record", x);
            }
            final ByteBuffer resultData = sink.getBuffer();
            resultData.flip();
            next.setData(resultData);
            LogBufferRecord removed = history.isFull() ? history.remove() : null;
            record(next);
            next = removed;
        }
    }
}

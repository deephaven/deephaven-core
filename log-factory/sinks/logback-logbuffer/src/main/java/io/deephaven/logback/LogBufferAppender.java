package io.deephaven.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.ErrorStatus;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.LogBufferRecord;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class LogBufferAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private LogBuffer logBuffer;
    private Encoder<ILoggingEvent> encoder;

    public void setEncoder(Encoder<ILoggingEvent> encoder) {
        this.encoder = Objects.requireNonNull(encoder);
    }

    @Override
    public void start() {
        int errors = 0;
        if (this.encoder == null) {
            addStatus(
                    new ErrorStatus("No encoder set for the appender named \"" + name + "\".", this));
            errors++;
        }

        this.logBuffer = LogBufferGlobal.getInstance().orElse(null);
        if (logBuffer == null) {
            addStatus(new ErrorStatus("No global LogBuffer found for \"" + name + "\".", this));
            errors++;
        }

        // only error free appenders should be activated
        if (errors == 0) {
            super.start();
        }
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event.getLevel() == Level.OFF) {
            return;
        }
        event.prepareForDeferredProcessing();

        // TODO (core#89): Add more structured data into LogBufferRecord
        final LogBufferRecord record = new LogBufferRecord();
        record.setLevel(adapt(event.getLevel()));
        record.setData(ByteBuffer.wrap(encoder.encode(event)));
        record.setTimestampMicros(event.getTimeStamp() * 1000);
        logBuffer.record(record);
    }

    private static LogLevel adapt(Level level) {
        switch (level.toInt()) {
            case Level.TRACE_INT:
            case Level.ALL_INT: // DH doesn't have the concept of "ALL" - trace is the finest, so
                                // we'll use that.
                return LogLevel.TRACE;

            case Level.DEBUG_INT:
                return LogLevel.DEBUG;

            case Level.INFO_INT:
                return LogLevel.INFO;

            case Level.WARN_INT:
                return LogLevel.WARN;

            case Level.ERROR_INT:
                return LogLevel.ERROR;

            default:
                throw new IllegalArgumentException(
                        "Unexpected level " + level + " " + level.toInt());
        }
    }
}

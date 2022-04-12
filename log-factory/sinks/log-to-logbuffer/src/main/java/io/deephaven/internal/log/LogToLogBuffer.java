package io.deephaven.internal.log;

import com.google.auto.service.AutoService;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferInterceptor;
import io.deephaven.io.logger.Logger;

@AutoService(InitSink.class)
public final class LogToLogBuffer implements InitSink {

    private static final Logger log = LoggerFactory.getLogger(LogToLogBuffer.class);

    @Override
    public void accept(LogSink sink, LogBuffer logBuffer) {
        log.info().append("Teeing ").append(Logger.class.getName()).append(" to LogBuffer").endl();
        sink.addInterceptor((LogBufferInterceptor) logBuffer);
    }
}

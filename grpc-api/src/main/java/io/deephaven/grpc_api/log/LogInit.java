package io.deephaven.grpc_api.log;

import io.deephaven.base.system.StandardStreamState;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.InitSink;
import io.deephaven.internal.log.LoggerFactory;

import javax.inject.Inject;
import java.io.UnsupportedEncodingException;
import java.util.Set;

public class LogInit {

    private static final Logger log = LoggerFactory.getLogger(LogInit.class);

    private final StandardStreamState standardStreamState;
    private final LogBuffer logBuffer;
    private final LogSink logSink;
    private final Set<InitSink> sinkInits;

    @Inject
    public LogInit(StandardStreamState standardStreamState, LogBuffer logBuffer, LogSink logSink,
            Set<InitSink> sinkInits) {
        this.standardStreamState = standardStreamState;
        this.logBuffer = logBuffer;
        this.logSink = logSink;
        this.sinkInits = sinkInits;
    }

    public void run() throws UnsupportedEncodingException {
        checkLogSinkIsSingleton();
        standardStreamState.setupRedirection();
        configureLoggerSink();
    }

    private void configureLoggerSink() {
        for (InitSink init : sinkInits) {
            init.accept(logSink, logBuffer);
        }
    }

    private void checkLogSinkIsSingleton() {
        if (log.getSink() != logSink) {
            // If this contract is broken, we'll need to start attaching interceptors at LoggerFactory
            // Logger creation time, or have some sort of mechanism for LoggerFactory to notify us about
            // new log creations.
            throw new RuntimeException(String.format("Logger impl %s does not work with the current implementation.",
                    log.getClass().getName()));
        }
    }
}

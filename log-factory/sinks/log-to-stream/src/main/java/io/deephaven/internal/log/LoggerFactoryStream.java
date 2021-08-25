package io.deephaven.internal.log;

import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import java.io.PrintStream;

public final class LoggerFactoryStream extends LoggerFactorySingleCache {

    private static PrintStream getStream() {
        final String value =
                System.getProperty("io.deephaven.internal.log.LoggerFactoryStream.stream", "OUT");
        switch (value.toUpperCase()) {
            case "OUT":
                return System.out;
            case "ERR":
                return System.err;
            default:
                throw new IllegalArgumentException("Unexpected stream " + value);
        }
    }

    private static LogLevel getLevel() {
        return LogLevel.valueOf(
                System.getProperty("io.deephaven.internal.log.LoggerFactoryStream.level", "INFO"));
    }

    @Override
    public final Logger createInternal() {
        return new StreamLoggerImpl(getStream(), getLevel());
    }
}

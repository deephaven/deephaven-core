//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;

import static io.deephaven.tablelogger.TableLogger.DEFAULT_INTRADAY_LOGGER_FLAGS;

/**
 * Logs data that describes JVM parameters. This is useful to check a worker's configuration.
 */
public interface ProcessInfoLogLogger {
    default void log(final String id, final String type, final String key, final String value) throws IOException {
        log(DEFAULT_INTRADAY_LOGGER_FLAGS, id, type, key, value);
    }

    void log(final Row.Flags flags, final String id, final String type, final String key, final String value)
            throws IOException;

    enum Noop implements ProcessInfoLogLogger {
        INSTANCE;

        @Override
        public void log(Flags flags, String id, String type, String key, String value) throws IOException {

        }
    }
}

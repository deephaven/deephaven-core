//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.tablelogger;

import java.io.Closeable;
import java.io.IOException;

/**
 * Logs data that describes JVM parameters. This is useful to check a worker's configuration.
 */
public interface ProcessInfoLogLogger extends Closeable {
    void log(final String id, final String type, final String key, final String value) throws IOException;

    enum Noop implements ProcessInfoLogLogger {
        INSTANCE;

        @Override
        public void log(String id, String type, String key, String value) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}

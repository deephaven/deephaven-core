package io.deephaven.engine.tablelogger;

import io.deephaven.tablelogger.Row;

import java.io.IOException;

public interface ProcessInfoLogLogger {
    void log(final String id, final String type, final String key, final String value) throws IOException;

    void log(final Row.Flags flags, final String id, final String type, final String key, final String value) throws IOException;
}

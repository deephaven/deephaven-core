/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

import java.io.IOException;

/**
 *
 * All generated TableLoggers implement this interface.
 *
 *
 */
public interface TableLogger {

    Row.Flags DEFAULT_INTRADAY_LOGGER_FLAGS = Row.Flags.SingleRow;

    void init(TableWriter tableWriter, int queueSize) throws IOException;

    boolean isClosed();

    /**
     * Close the logger and any writer in use. Users should quiesce all logging threads and invoke
     * {@link #shutdown()} first in order to guarantee that all pending rows have been written to
     * storage.
     *
     * @throws IOException if an error occurred closing the logger.
     */
    void close() throws IOException;

    /**
     * Write all enqueued elements to the {@link TableWriter} and prevent further writes. This
     * should be invoked before {@link #close()}. This must not be invoked if any threads might
     * still try to log additional items.
     */
    void shutdown();
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream implementation for outputting data to a Logger. Expects that each appended byte
 * represents a char (1:1, so only ASCII/LATIN-1 and similar charsets are supported). flush() events
 * are ignored - we end() log entries on newlines.
 */
public class LoggerOutputStream extends OutputStream {

    private final Logger logger;
    private final LogLevel logLevel;

    private LogEntry currentEntry;
    private int currentSize;

    /**
     * Create a stream that writes bytes as chars to the specified logger at the specified level.
     * 
     * @param logger
     * @param logLevel
     */
    public LoggerOutputStream(Logger logger, LogLevel logLevel) {
        this.logger = logger;
        this.logLevel = logLevel;
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        final char c = (char) (b & 0xff);
        if (currentEntry == null) {
            getNewEntry();
        }
        currentSize++;

        /*
         * This class is primarily used to capture data to be sent to the ProcessEventLog. There are
         * two restrictions on the size of what can be captured. 1. The number of available buffers.
         * If we obtain all the buffers, we hang in ThreadSafeFixedSizePool trying to get a buffer
         * that will never be available. By default this is 2048 buffers of 1K each. 2. The maximum
         * size of a binary log entry. This is defined by BinaryStoreV2Constants.MAX_ENTRY_SIZE,
         * currently defined at 1024 * 1024. To be safe, we'll start splitting messages at 512K.
         */
        if (currentSize >= 524_288) {
            currentEntry.end();
            getNewEntry();
        }

        currentEntry = currentEntry.append(c);

        if (c == '\n') {
            currentEntry.end();
            currentEntry = null;
        }
    }

    private void getNewEntry() {
        currentEntry = logger.getEntry(logLevel);
        currentSize = 0;
    }
}

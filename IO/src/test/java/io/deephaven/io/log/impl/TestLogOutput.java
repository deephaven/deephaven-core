/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StringsLoggerImpl;
import junit.framework.TestCase;


/**
 * TODO: This class never existed until the potential NPE bug when logging boxed primitives that are null was TODO:
 * exposed, so those cases are the only ones tested here. But this should be expanded to include all TODO: LogOutput
 * methods, and since StringsLoggerImpl can easily be instantiated with arbitrary LogEntry TODO: implenmentations, it
 * should also be made into an abstract base class with concrete subclasses for TODO: entry entry impl.
 */
public class TestLogOutput extends TestCase {
    LogBufferPoolImpl buffers;
    LogOutput outputBuffer;
    StringsLoggerImpl<? extends LogEntry> logger;


    public void setUp() throws Exception {
        super.setUp();
        this.buffers = new LogBufferPoolImpl(16, 256);
        this.outputBuffer = new LogOutputCsvImpl(buffers);

        // Function.Nullary<LogEntry> entryFactory = DelayedLogEntryUnsafeImpl::new;
        Function.Nullary<LogEntry> entryFactory = () -> new LogEntryImpl(buffers);

        this.logger = new StringsLoggerImpl<>(entryFactory, 16, outputBuffer, LogLevel.INFO);
    }

    public void testInteger() {
        logger.info().append((Integer) null).end();
        logger.info().append((Integer) 123456).end();

        String[] results = logger.takeAll();
        assertEquals("null", results[0]);
        assertEquals("123456", results[1]);

        logger.info().append((Integer) 123456).end();

        results = logger.takeAll();
        assertEquals("123456", results[0]);
    }

    public void testBoolean() {
        logger.info().append((Boolean) null).end();
        logger.info().append((Boolean) true).end();
        logger.info().append((Boolean) false).end();

        String[] results = logger.takeAll();
        assertEquals("null", results[0]);
        assertEquals("true", results[1]);
        assertEquals("false", results[2]);

        logger.info().append(true).end();
        logger.info().append(false).end();
        results = logger.takeAll();
        assertEquals("true", results[0]);
        assertEquals("false", results[1]);
    }

    public void testPositiveDoubleToDecimalPlaces() {
        logger.info().appendDouble(1.2345, 3).end();
        logger.info().appendDouble(0.112255, 2).end();
        logger.info().appendDouble(11111111.112255, 3).end();
        logger.info().appendDouble(11111111.112255, 4).end();
        logger.info().appendDouble(1111.4, 0).end();
        logger.info().appendDouble(1111.5, 0).end();
        logger.info().appendDouble(111.1234567894, 9).end();
        logger.info().appendDouble(111.1234567895, 9).end();
        logger.info().appendDouble(111.1234567895, 9, 9).end();
        logger.info().appendDouble(111.123456789, 9).end();
        logger.info().appendDouble(111.123456789, 9, 9).end();
        logger.info().appendDouble(111.12, 4).end();
        logger.info().appendDouble(111.12, 4, 4).end();
        logger.info().appendDouble(111.14, 2).end();
        logger.info().appendDouble(111.15, 2).end();
        logger.info().appendDouble(111.15, 0).end();
        logger.info().appendDouble(0, 0).end();
        logger.info().appendDouble(0, 3).end();
        logger.info().appendDouble(111.1995, 3).end();
        logger.info().appendDouble(111.1995, 3, 1).end();
        logger.info().appendDouble(111.1995, 3, 2).end();
        logger.info().appendDouble(111.1995, 3, 3).end();
        String[] results = logger.takeAll();
        int c = 0;
        assertEquals("1.235", results[c++]);
        assertEquals("0.11", results[c++]);
        assertEquals("11111111.112", results[c++]);
        assertEquals("11111111.1123", results[c++]);
        assertEquals("1111", results[c++]);
        assertEquals("1112", results[c++]);
        assertEquals("111.123456789", results[c++]);
        assertEquals("111.123456790", results[c++]);
        assertEquals("111.12345679", results[c++]);
        assertEquals("111.123456789", results[c++]);
        assertEquals("111.123456789", results[c++]);
        assertEquals("111.1200", results[c++]);
        assertEquals("111.12", results[c++]);
        assertEquals("111.14", results[c++]);
        assertEquals("111.15", results[c++]);
        assertEquals("111", results[c++]);
        assertEquals("0", results[c++]);
        assertEquals("0.000", results[c++]);
        assertEquals("111.200", results[c++]);
        assertEquals("111.20", results[c++]);
        assertEquals("111.2", results[c++]);
        assertEquals("111.2", results[c++]);
    }

    public void testNegativeDoubleToDecimalPlaces() {
        logger.info().appendDouble(-1.235, 2).end();
        logger.info().appendDouble(-1.234, 2).end();
        String[] results = logger.takeAll();
        int c = 0;
        assertEquals("-1.24", results[c++]);
        assertEquals("-1.23", results[c++]);
    }
}

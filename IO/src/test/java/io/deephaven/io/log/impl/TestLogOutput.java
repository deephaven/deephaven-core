package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StringsLoggerImpl;
import junit.framework.TestCase;


/**
 * TODO: This class never existed until the potential NPE bug when logging boxed primitives that are
 * null was TODO: exposed, so those cases are the only ones tested here. But this should be expanded
 * to include all TODO: LogOutput methods, and since StringsLoggerImpl can easily be instantiated
 * with arbitrary LogEntry TODO: implenmentations, it should also be made into an abstract base
 * class with concrete subclasses for TODO: entry entry impl.
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
}

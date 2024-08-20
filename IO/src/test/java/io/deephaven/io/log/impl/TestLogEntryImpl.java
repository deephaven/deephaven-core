//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;

import java.nio.ByteBuffer;

public class TestLogEntryImpl extends TestCase {

    Mockery context;
    LogBufferPool pool;
    LogSink sink;

    public void setUp() throws Exception {
        context = new Mockery();
        pool = context.mock(LogBufferPool.class);
        sink = context.mock(LogSink.class);
    }

    public void testStartWritten() {
        final LogEntryImpl SUT = new LogEntryImpl(pool);
        final ByteBuffer buf = ByteBuffer.allocate(LogOutputBaseImpl.START_SIZE_BYTES);

        context.checking(new Expectations() {
            {
                oneOf(pool).take(LogOutputBaseImpl.START_SIZE_BYTES);
                will(returnValue(buf));
            }
        });
        SUT.start(sink, LogLevel.INFO);
        context.assertIsSatisfied();

        context.checking(new Expectations() {
            {
                oneOf(sink).write(with(same(SUT)));
                will(new CustomAction("write") {
                    public Object invoke(Invocation invocation) throws Throwable {
                        SUT.writing(null);
                        SUT.written(null);
                        return null;
                    }
                });
                oneOf(pool).give(with(same(buf)));
            }
        });
        SUT.append("FOO!");
        SUT.endl();
        context.assertIsSatisfied();
    }
}

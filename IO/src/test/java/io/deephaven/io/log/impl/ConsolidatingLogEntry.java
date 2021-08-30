/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.LogSink;

import java.nio.ByteBuffer;

// --------------------------------------------------------------------
/**
 * A mock {@link LogEntry} for use with JMock that consolidates all the append calls into the single resulting string.
 * <P>
 * Example:
 * </P>
 * 
 * <PRE>
 * final Logger logger = mock(Logger.class);
 * final ConsolidatingLogEntry.Monitor logEntryMonitor = mock(ConsolidatingLogEntry.Monitor.class);
 * final LogEntry logEntry = new ConsolidatingLogEntry(logEntryMonitor);
 * checking(new Expectations() {
 *     {
 *         oneOf(logger).info();
 *         will(returnValue(logEntry));
 *         oneOf(logEntryMonitor).endl("Some logged message.");
 *     }
 * });
 * objectUnderTest.someMethodThatLogs(logger);
 * assertIsSatisfied();
 * </PRE>
 */
public class ConsolidatingLogEntry extends LogOutputStringImpl implements LogEntry {

    public interface Monitor {
        /**
         * Indicates that {@link LogEntry#endl()} was called on the monitored {@link LogEntry} and the given message had
         * been accumulated since the last call to endl().
         */
        void endl(String sMessage);

        void start(LogSink sink, LogLevel level, Throwable t);
    }

    private final Monitor m_monitor;

    public ConsolidatingLogEntry(Monitor monitor) {
        Require.neqNull(monitor, "monitor");
        m_monitor = monitor;
    }

    // called in JMock error reports, this makes more sense in that context that the super impl.
    @Override
    public String toString() {
        return "{ConsolidatingLogEntry}";
    }

    // ################################################################

    @Override
    public LogEntry start(final LogSink sink, LogLevel level) {
        start(sink, level, System.currentTimeMillis() * 1000, null);
        return this;
    }

    @Override
    public LogEntry start(final LogSink sink, LogLevel level, long currentTimeMicros) {
        start(sink, level, currentTimeMicros, null);
        return this;
    }

    @Override // from LogEntry
    public LogEntry start(LogSink sink, LogLevel level, long currentTimeMicros, Throwable t) {
        m_monitor.start(sink, level, t);
        return this;
    }

    @Override // from LogEntry
    public LogEntry end() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogEntry
    public LogEntry endl() {
        m_monitor.endl(builder.toString());
        builder.setLength(0);
        return this;
    }

    // ################################################################

    @Override // from LogSink.Element
    public long getTimestampMicros() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public LogLevel getLevel() {
        throw Assert.statementNeverExecuted();
    }

    @Override
    public Throwable getThrowable() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogSink.Element
    public LogOutput writing(LogOutput outputBuffer) {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogSink.Element
    public void written(LogOutput outputBuffer) {
        throw Assert.statementNeverExecuted();
    }

    // ################################################################

    @Override // from LogOutput
    public LogOutput start() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public LogOutput close() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public int relativeSize() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public int size() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public int getBufferCount() {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public ByteBuffer getBuffer(int i) {
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public LogOutput clear() {
        throw Assert.statementNeverExecuted();
    }

    // ################################################################

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final boolean b) {
        super.append(b);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final char c) {
        super.append(c);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final short s) {
        super.append(s);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final int i) {
        super.append(i);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final long l) {
        super.append(l);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry appendDouble(final double f) {
        super.appendDouble(f);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final LogOutputAppendable appendable) {
        super.append(appendable);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(LongFormatter formatter, long n) {
        super.append(formatter, n);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public <T> LogEntry append(ObjFormatter<T> objFormatter, T t) {
        super.append(objFormatter, t);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public <T> LogEntry append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
        super.append(objFormatter, t, nOffset, nLength);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public <T, U> LogEntry append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
        objObjFormatter.format(this, t, u);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final CharSequence seq) {
        super.append(seq);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final CharSequence seq, final int start, final int length) {
        super.append(seq, start, length);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(final ByteBuffer buffer) {
        super.append(buffer);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
        super.appendTimestamp(utcMillis, tb);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
        super.appendTimestampMicros(utcMicros, tb);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(Throwable t) {
        super.append(t);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(byte[] ba) {
        super.append(ba);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(byte[] ba, int pos, int length) {
        super.append(ba, pos, length);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry append(byte[] ba, byte terminator) {
        super.append(ba, terminator);
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry nf() {
        super.nf();
        return this;
    }

    @Override // from LogEntry, covariant with LogOutput
    public LogEntry nl() {
        super.nl();
        return this;
    }
}

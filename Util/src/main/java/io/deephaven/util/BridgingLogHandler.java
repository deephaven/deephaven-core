/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.impl.LogOutputStringImpl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public final class BridgingLogHandler extends Handler {
    private static final boolean FLUSH_PENDING_ON_SHUTDOWN_DEFAULT = true;
    private static boolean flushPendingOnShutdown;
    static {
        // Tools that don't want this in their output should set this to false.
        final String propValue = System.getProperty("BridgingLogHandler.flushPendingOnShutdown");
        flushPendingOnShutdown = (propValue == null) ? FLUSH_PENDING_ON_SHUTDOWN_DEFAULT
            : Boolean.parseBoolean(propValue);
    }

    public static synchronized void setFlushPendingOnShutdown(final boolean v) {
        flushPendingOnShutdown = v;
    }

    private static class PendingLogRecord {
        public final Level level;
        public final long timeMillis;
        public final Throwable throwable;
        public final String sourceClassName;
        public final String sourceMethodName;
        public final int threadId;
        public final String msg;

        public PendingLogRecord(
            final Level level,
            final long timeMillis,
            final Throwable throwable,
            final String sourceClassName,
            final String sourceMethodName,
            final int threadId,
            final String msg) {
            this.level = level;
            this.throwable = throwable;
            this.timeMillis = timeMillis;
            this.sourceClassName = sourceClassName;
            this.sourceMethodName = sourceMethodName;
            this.threadId = threadId;
            this.msg = msg;
        }
    }

    private static ArrayList<PendingLogRecord> pending = null;
    private static volatile io.deephaven.io.logger.Logger log = null;

    public static void setLogger(io.deephaven.io.logger.Logger log) {
        synchronized (BridgingLogHandler.class) {
            try {
                if (pending == null) {
                    return;
                }
                for (PendingLogRecord p : pending) {
                    final LogEntry entry = logEntry(log, p.level, p.timeMillis, p.throwable);
                    appendPendingLogRecord(entry, p);
                    entry.endl();
                }
                BridgingLogHandler.pending = null;
            } finally {
                BridgingLogHandler.log = log;
            }
        }
    }

    private static void pushRecord(final LogRecord logRecord) {
        // Compute these early as they can be expensive.
        final String sourceClassName = logRecord.getSourceClassName();
        final String sourceMethodName = logRecord.getSourceMethodName();
        if (log == null) {
            synchronized (BridgingLogHandler.class) {
                if (log == null) {
                    if (pending == null) {
                        pending = new ArrayList<>();
                        if (flushPendingOnShutdown) {
                            Runtime.getRuntime().addShutdownHook(
                                new Thread(BridgingLogHandler::pushPendingToStdout));
                        }
                    }
                    pending.add(new PendingLogRecord(
                        logRecord.getLevel(),
                        logRecord.getMillis(),
                        logRecord.getThrown(),
                        sourceClassName,
                        sourceMethodName,
                        logRecord.getThreadID(),
                        logRecord.getMessage()));
                    return;
                }
            }
        }
        final LogEntry entry =
            logEntry(log, logRecord.getLevel(), logRecord.getMillis(), logRecord.getThrown());
        appendMsg(entry, sourceClassName, sourceMethodName,
            logRecord.getThreadID(), logRecord.getMessage());
        entry.endl();
    }

    public static void appendPendingLogRecord(
        final LogOutput logOutput, final PendingLogRecord pendingLogRecord) {
        appendMsg(logOutput,
            pendingLogRecord.sourceClassName,
            pendingLogRecord.sourceMethodName,
            pendingLogRecord.threadId,
            pendingLogRecord.msg);
    }

    private static void appendMsg(LogOutput logOutput,
        final String sourceClassName, final String sourceMethodName, final int threadId,
        final String msg) {
        logOutput.append("[")
            .append(sourceClassName)
            .append(":")
            .append(sourceMethodName)
            .append(":tid=")
            .append(threadId)
            .append("] ")
            .append(msg);
    }

    private static LogEntry logEntry(
        final io.deephaven.io.logger.Logger log, final Level level, final long timeMillis,
        final Throwable throwable) {
        return log.getEntry(mapLevel(level), 1000 * timeMillis, throwable);
    }

    private static final Map<Level, LogLevel> LEVEL_MAPPINGS = new HashMap<>();
    static {
        LEVEL_MAPPINGS.put(Level.WARNING, LogLevel.WARN);
        LEVEL_MAPPINGS.put(Level.SEVERE, LogLevel.ERROR);
    }

    private static io.deephaven.io.log.LogLevel mapLevel(final Level level) {
        final LogLevel mapping = LEVEL_MAPPINGS.get(level);
        if (mapping != null) {
            return mapping;
        }
        return LogLevel.INFO;
    }

    private static void pushPendingToStdout() {
        final ArrayList<PendingLogRecord> pending;
        final boolean doFlush;
        synchronized (BridgingLogHandler.class) {
            pending = BridgingLogHandler.pending;
            BridgingLogHandler.pending = null;
            doFlush = flushPendingOnShutdown;
        }
        if (pending == null || !doFlush) {
            return;
        }
        final LogOutput logOutput = new LogOutputStringImpl();
        for (PendingLogRecord p : pending) {
            // Instant's toString formats as ISO-8601.
            final String timeStampStr = Instant.ofEpochMilli(p.timeMillis).toString();
            logOutput.append(timeStampStr).append(" ");
            appendPendingLogRecord(logOutput, p);
            logOutput.append("\n");
        }
        System.out.println("Flushing pending log records on shutdown.");
        System.out.print(logOutput.toString());
    }

    @Override
    public void publish(final LogRecord record) {
        if (!isLoggable(record)) {
            return;
        }
        pushRecord(record);
    }

    @Override
    public void flush() {
        // do nothing.
    }

    @Override
    public void close() throws SecurityException {
        // do nothing.
    }
}

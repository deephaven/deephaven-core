/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io;

import io.deephaven.io.log.*;
import io.deephaven.io.log.impl.LogBufferPoolImpl;
import io.deephaven.io.log.impl.LogEntryPoolImpl;
import io.deephaven.io.log.impl.LogSinkImpl;

public class ExerciseLogSink {

    public static void main(String[] args) {
        LogBufferPool bufferPool = new LogBufferPoolImpl(1000, 512);
        LogEntryPool entryPool = new LogEntryPoolImpl(1000, bufferPool);
        LogSink sink = new LogSinkImpl<LogEntry>("/tmp/test1.log", 5000, entryPool);

        int entries = 0;
        for (int i = 0; i < 10000; ++i) {
            int n = (int) (Math.random() * 10) + 1;
            for (int j = 0; j < n; ++j) {
                LogEntry e = entryPool.take().start(sink, LogLevel.INFO);
                e.append("Log: iteration ").append(i).append(" entry ").append(j);
                e.endl();
                entries++;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException x) {
                // ignore
            }
        }
        System.out.println("ExerciseLogSink: wrote " + entries + " entries");
        sink.shutdown();
    }
}

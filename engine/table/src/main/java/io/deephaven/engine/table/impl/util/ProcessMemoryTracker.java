package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.tablelogger.ProcessMemoryLogLogger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.IOException;

public class ProcessMemoryTracker {
    private static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            ProcessMemoryTracker.class, "reportIntervalMillis", 15 * 1000L);

    private static volatile ProcessMemoryTracker INSTANCE;
    private static boolean started = false;

    public static ProcessMemoryTracker getInstance() {
        if (INSTANCE == null) {
            synchronized (ProcessMemoryTracker.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProcessMemoryTracker();
                }
            }
        }
        return INSTANCE;
    }

    private final Logger logger;

    private final MemoryTableLogger<ProcessMemoryLogLogger> processMemLogger;

    private ProcessMemoryTracker() {
        logger = LoggerFactory.getLogger(ProcessMemoryTracker.class);
        processMemLogger = new MemoryTableLogger<>(
                logger, new ProcessMemoryLogLogger(), ProcessMemoryLogLogger.getTableDefinition());
    }

    private void startThread() {
        Thread driverThread = new Thread(new ProcessMemoryTracker.Driver(), "ProcessMemoryTracker.Driver");
        driverThread.setDaemon(true);
        driverThread.start();
    }

    public static synchronized void start() {
        if (started) {
            return;
        }
        started = true;
        getInstance().startThread();
    }

    private class Driver implements Runnable {
        @Override
        public void run() {
            final RuntimeMemory.Sample memSample = new RuntimeMemory.Sample();
            // noinspection InfiniteLoopStatement
            while (true) {
                final long intervalStartTimeMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(REPORT_INTERVAL_MILLIS);
                } catch (InterruptedException ignore) {
                    // should log, but no logger handy
                    // ignore
                }
                final long prevTotalCollections = memSample.totalCollections;
                final long prevTotalCollectionTimeMs = memSample.totalCollectionTimeMs;
                RuntimeMemory.getInstance().read(memSample);
                final long endTimeMillis = System.currentTimeMillis();
                logProcessMem(
                        intervalStartTimeMillis,
                        endTimeMillis,
                        memSample,
                        prevTotalCollections,
                        prevTotalCollectionTimeMs);
            }
        }
    }

    private void logProcessMem(
            final long startMillis, final long endMillis,
            final RuntimeMemory.Sample sample,
            final long prevTotalCollections, final long prevTotalCollectionTimeMs) {
        try {
            processMemLogger.getTableLogger().log(
                    startMillis,
                    DateTimeUtils.millisToNanos(endMillis - startMillis),
                    sample.totalMemory,
                    sample.freeMemory,
                    sample.totalCollections - prevTotalCollections,
                    DateTimeUtils.millisToNanos(sample.totalCollectionTimeMs - prevTotalCollectionTimeMs));
        } catch (IOException e) {
            // Don't want to log this more than once in a report
            logger.error().append("Error sending ProcessMemoryLog data to memory").append(e).endl();
        }
    }

    public QueryTable getQueryTable() {
        return processMemLogger.getQueryTable();
    }
}

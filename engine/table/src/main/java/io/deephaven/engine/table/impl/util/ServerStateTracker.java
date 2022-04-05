package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.tablelogger.ServerStateLog;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;

public class ServerStateTracker {
    private static final long REPORT_INTERVAL_MILLIS = Configuration.getInstance().getLongForClassWithDefault(
            ServerStateTracker.class, "reportIntervalMillis", 15 * 1000L);

    private static volatile ServerStateTracker INSTANCE;
    private static boolean started = false;

    public static ServerStateTracker getInstance() {
        if (INSTANCE == null) {
            synchronized (ServerStateTracker.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ServerStateTracker();
                }
            }
        }
        return INSTANCE;
    }

    private final Logger logger;

    private final MemoryTableLogger<ServerStateLog> processMemLogger;
    private final UpdateGraphProcessor.AccumulatedCycleStats ugpAccumCycleStats;

    private ServerStateTracker() {
        logger = LoggerFactory.getLogger(ServerStateTracker.class);
        processMemLogger = new MemoryTableLogger<>(
                logger, new ServerStateLog(), ServerStateLog.getTableDefinition());
        ugpAccumCycleStats = new UpdateGraphProcessor.AccumulatedCycleStats();
    }

    private void startThread() {
        Thread driverThread = new Thread(
                new ServerStateTracker.Driver(),
                ServerStateTracker.class.getSimpleName() + ".Driver");
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
                final long prevUgpCycles = ugpAccumCycleStats.getTotalCycles();
                final long prevUgpCyclesOnBudget = ugpAccumCycleStats.getTotalCyclesOnBudget();
                final long prevUgpCyclesTimeNanos = ugpAccumCycleStats.getTotalCyclesTimeNanos();
                final long prevUgpSafePOintPauseTimeMillis = ugpAccumCycleStats.getTotalSafePointPauseTimeMillis();
                UpdateGraphProcessor.DEFAULT.accumulatedCycleStats.copyTo(ugpAccumCycleStats);
                final long endTimeMillis = System.currentTimeMillis();
                logProcessMem(
                        intervalStartTimeMillis,
                        endTimeMillis,
                        memSample,
                        prevTotalCollections,
                        prevTotalCollectionTimeMs,
                        ugpAccumCycleStats.getTotalCycles() - prevUgpCycles,
                        ugpAccumCycleStats.getTotalCyclesOnBudget() - prevUgpCyclesOnBudget,
                        ugpAccumCycleStats.getTotalCyclesTimeNanos() - prevUgpCyclesTimeNanos,
                        ugpAccumCycleStats.getTotalSafePointPauseTimeMillis() - prevUgpSafePOintPauseTimeMillis);
            }
        }
    }

    private void logProcessMem(
            final long startMillis, final long endMillis,
            final RuntimeMemory.Sample sample,
            final long prevTotalCollections,
            final long prevTotalCollectionTimeMs,
            final long ugpCycles,
            final long ugpCyclesOnBudget,
            final long ugpCyclesNanos,
            final long ugpSafePointTimeMillis) {
        try {
            processMemLogger.getTableLogger().log(
                    startMillis,
                    DateTimeUtils.millisToNanos(endMillis - startMillis),
                    sample.totalMemory,
                    sample.freeMemory,
                    sample.totalCollections - prevTotalCollections,
                    DateTimeUtils.millisToNanos(sample.totalCollectionTimeMs - prevTotalCollectionTimeMs),
                    ugpCycles,
                    ugpCyclesOnBudget,
                    ugpCyclesNanos,
                    DateTimeUtils.millisToNanos(ugpSafePointTimeMillis));
        } catch (IOException e) {
            // Don't want to log this more than once in a report
            logger.error().append("Error sending ProcessMemoryLog data to memory").append(e).endl();
        }
    }

    public QueryTable getQueryTable() {
        return processMemLogger.getQueryTable();
    }
}

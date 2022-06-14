/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.tablelogger.ServerStateLog;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;

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

    static class Stats {
        long max;
        long mean;
        long median;
        long p90;
    }

    static void calcStats(final Stats out, final long[] values, final int nValues) {
        if (nValues == 0) {
            out.max = out.mean = out.median = out.p90 = NULL_LONG;
            return;
        }
        Arrays.sort(values, 0, nValues);
        out.max = values[nValues - 1];
        if ((nValues & 1) == 0) {
            // even number of samples
            final int midRight = nValues / 2;
            out.median = Math.round((values[midRight - 1] + values[midRight]) / 2.0);
        } else {
            // odd number of samples
            out.median = values[nValues / 2];
        }
        double sum = 0.0;
        for (int i = 0; i < nValues; ++i) {
            sum += values[i];
        }
        out.mean = Math.round(sum / nValues);
        final double p90pos = nValues * 0.9 - 1.0;
        if (p90pos < 0) {
            out.p90 = values[0];
        } else {
            final int p90posTruncated = (int) p90pos;
            final double p90posDelta = p90pos - p90posTruncated;
            // Note we are approximating via the 'higher' method,
            // using the next up actual sample in the array if the
            // percentile position does not exist "exactly".
            // see numpy's percentile documentation for alternatives
            // we could use here (eg, linear interpolation).
            // for our current objectives this should suffice.
            if (p90posDelta >= 1e-2 && p90posTruncated < nValues - 1) {
                out.p90 = values[p90posTruncated + 1];
            } else {
                out.p90 = values[p90posTruncated];
            }
        }
    }

    private class Driver implements Runnable {
        @Override
        public void run() {
            final RuntimeMemory.Sample memSample = new RuntimeMemory.Sample();
            // noinspection InfiniteLoopStatement
            while (true) {
                final Stats stats = new Stats();
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
                UpdateGraphProcessor.DEFAULT.accumulatedCycleStats.take(ugpAccumCycleStats);
                final long endTimeMillis = System.currentTimeMillis();
                logProcessMem(
                        intervalStartTimeMillis,
                        endTimeMillis,
                        memSample,
                        prevTotalCollections,
                        prevTotalCollectionTimeMs,
                        ugpAccumCycleStats.cycles,
                        ugpAccumCycleStats.cyclesOnBudget,
                        ugpAccumCycleStats.cycleTimesMicros,
                        ugpAccumCycleStats.safePoints,
                        ugpAccumCycleStats.safePointPauseTimeMillis);
            }
        }
    }

    private int deltaMillisToMicros(final long millis) {
        final long result = millis * 1000;
        return (int) result;
    }

    private int bytesToMiB(final long bytes) {
        final long mib = (bytes + 512 * 1024) / (1024 * 1024);
        return (int) mib;
    }

    private void logProcessMem(
            final long startMillis, final long endMillis,
            final RuntimeMemory.Sample sample,
            final long prevTotalCollections,
            final long prevTotalCollectionTimeMs,
            final int ugpCycles,
            final int ugpCyclesOnBudget,
            final int[] ugpCycleTimes,
            final int ugpSafePoints,
            final long ugpSafePointTimeMillis) {
        try {
            processMemLogger.getTableLogger().log(
                    startMillis,
                    deltaMillisToMicros(endMillis - startMillis),
                    bytesToMiB(sample.totalMemory),
                    bytesToMiB(sample.freeMemory),
                    (short) (sample.totalCollections - prevTotalCollections),
                    deltaMillisToMicros(sample.totalCollectionTimeMs - prevTotalCollectionTimeMs),
                    (short) ugpCyclesOnBudget,
                    Arrays.copyOf(ugpCycleTimes, ugpCycles),
                    (short) ugpSafePoints,
                    deltaMillisToMicros(ugpSafePointTimeMillis));
        } catch (IOException e) {
            // Don't want to log this more than once in a report
            logger.error().append("Error sending ProcessMemoryLog data to memory").append(e).endl();
        }
    }

    public QueryTable getQueryTable() {
        return processMemLogger.getQueryTable();
    }
}

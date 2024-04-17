//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stats;

import io.deephaven.base.clock.Clock;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.util.SafeCloseable;
import io.deephaven.base.stats.*;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.*;
import io.deephaven.io.log.impl.LogEntryPoolImpl;
import io.deephaven.io.log.impl.LogSinkImpl;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.thread.NamingThreadFactory;

import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Drives the collection of statistics on a 1-second timer task.
 */
public class StatsDriver {
    public interface StatusAdapter {
        void sendAlert(String alertText);

        boolean cmsAlertEnabled();

        class Null implements StatusAdapter {
            public void sendAlert(String unused) {}

            public boolean cmsAlertEnabled() {
                return true;
            }
        }

        StatusAdapter NULL = new Null();
    }

    private final LogEntryPool entryPool;
    private final LogSink<?> sink;
    private final LogEntry[] entries;

    private final LogEntryPool entryPoolHisto;
    private final LogSink<?> sinkHisto;
    private final LogEntry[] entriesHisto;

    private final TimestampBuffer systemTimestamp;
    private final TimestampBuffer appTimestamp;

    public final static String header =
            "Stat,IntervalName,NowSec,NowString,AppNowSec,AppNowString,TypeTag,Name,N,Sum,Last,Min,Max,Avg,Sum2,Stdev";

    private long nextCpuUpdate;
    private long nextMemUpdate;

    private static final long STEP = 1000;
    private static final long MEM_INTERVAL = 1000;
    private static final long CPU_INTERVAL = 1000;
    private static final long REPORT_INTERVAL = 10000;

    private static final int BUFFER_SIZE = 8192;
    private static final int GUESS_ENTRY_SIZE = 256;

    private final Value statsTiming = Stats.makeItem("Stats", "updateDuration", State.FACTORY,
            "Microseconds required to update the statistics histories each second").getValue();

    private final Clock clock;
    private final StatsIntradayLogger intraday;
    private final Value clockValue;
    private final ExecutionContext executionContext;
    @ReferentialIntegrity
    private final ScheduledExecutorService scheduler;
    @ReferentialIntegrity
    private final ScheduledFuture<?> updateJobFuture;

    private final StatsMemoryCollector memStats;
    private final StatsCPUCollector cpuStats;
    @ReferentialIntegrity
    private ObjectAllocationCollector objectAllocation;

    public StatsDriver(Clock clock) {
        this(clock, true, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatusAdapter statusAdapter) {
        this(clock, true, statusAdapter);
    }

    public StatsDriver(Clock clock, boolean getFdStats, StatusAdapter statusAdapter) {
        this(clock, StatsIntradayLogger.NULL, getFdStats, statusAdapter);
    }

    public StatsDriver(Clock clock, boolean getFdStats) {
        this(clock, StatsIntradayLogger.NULL, getFdStats, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatsIntradayLogger intraday, boolean getFdStats) {
        this(clock, intraday, getFdStats, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatsIntradayLogger intraday, boolean getFdStats, StatusAdapter statusAdapter) {
        Properties props = Configuration.getInstance().getProperties();
        String path = props.getProperty("stats.log.prefix");
        if (path != null) {
            if (path.isEmpty()) {
                path = null;
            } else {
                path += ".stats";
            }
        }

        String histoPath = props.getProperty("histo.log.prefix");
        if (histoPath != null) {
            if (histoPath.isEmpty()) {
                histoPath = null;
            } else {
                histoPath += ".histo";
            }
        }

        final TimeZone serverTimeZone = Configuration.getInstance().getServerTimezone();
        this.systemTimestamp = new TimestampBuffer(serverTimeZone);
        this.appTimestamp = new TimestampBuffer(serverTimeZone);

        if (path == null) {
            this.entryPool = null;
            this.sink = null;
            this.entries = null;
        } else {
            LogBufferPool bufferPool = LogBufferPool.ofStrict(History.INTERVALS.length * 20, BUFFER_SIZE);
            this.entryPool = new LogEntryPoolImpl(History.INTERVALS.length * 20, bufferPool);
            this.sink = new LogSinkImpl<>(path, 3600 * 1000, entryPool, true);
            this.entries = new LogEntry[History.INTERVALS.length];
        }

        if (histoPath == null) {
            this.entryPoolHisto = null;
            this.sinkHisto = null;
            this.entriesHisto = null;
        } else {
            LogBufferPool bufferPool = LogBufferPool.ofStrict(History.INTERVALS.length * 20, BUFFER_SIZE);
            this.entryPoolHisto = new LogEntryPoolImpl(History.INTERVALS.length * 20, bufferPool);
            this.sinkHisto = new LogSinkImpl<>(histoPath, 3600 * 1000, entryPoolHisto, true);
            this.entriesHisto = new LogEntry[History.INTERVALS.length];
        }

        this.clock = clock;
        this.intraday = intraday;
        if (this.clock != null) {
            clockValue = Stats.makeItem("Clock", "value", State.FACTORY,
                    "The value of the Clock, useful for mapping data from simulation runs").getValue();
        } else {
            clockValue = null;
        }

        final long now = System.currentTimeMillis();
        final long delay = STEP - (now % STEP);
        nextCpuUpdate = now + delay + CPU_INTERVAL;
        nextMemUpdate = now + delay + MEM_INTERVAL;

        cpuStats = new StatsCPUCollector(CPU_INTERVAL, getFdStats);
        memStats = new StatsMemoryCollector(MEM_INTERVAL, statusAdapter::sendAlert, statusAdapter::cmsAlertEnabled);
        if (Configuration.getInstance().getBoolean("allocation.stats.enabled")) {
            objectAllocation = new ObjectAllocationCollector();
        }
        executionContext = ExecutionContext.getContext();

        // now that the StatsDriver is completely constructed, we can schedule the update job
        if (Configuration.getInstance().getBoolean("statsdriver.enabled")) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    new NamingThreadFactory(StatsDriver.class, "updateScheduler", true));
            updateJobFuture = scheduler.scheduleAtFixedRate(this::update, delay, STEP, TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
            updateJobFuture = null;
        }
    }

    private void update() {
        long t0 = System.nanoTime();
        long now = System.currentTimeMillis();
        long appNow = clock == null ? now : clock.currentTimeMillis();

        if (now >= nextCpuUpdate) {
            nextCpuUpdate += CPU_INTERVAL;
            cpuStats.update();
        }

        if (now >= nextMemUpdate) {
            nextMemUpdate += MEM_INTERVAL;
            memStats.update();
        }

        if (clock != null) {
            clockValue.sample(clock.currentTimeMillis());
        }

        if (this.entries == null) {
            try (final SafeCloseable ignored = executionContext.open()) {
                Stats.update(LISTENER, now, appNow, REPORT_INTERVAL);
            }
        } else {
            for (int i = 0; i < History.INTERVALS.length; ++i) {
                entries[i] = entryPool.take().start(sink, LogLevel.INFO, now * 1000);
                if (entriesHisto != null) {
                    entriesHisto[i] = entryPoolHisto.take().start(sinkHisto, LogLevel.INFO, now * 1000);
                }
            }
            try (final SafeCloseable ignored = executionContext.open()) {
                Stats.update(LISTENER, now, appNow, REPORT_INTERVAL);
            }
            for (int i = 0; i < History.INTERVALS.length; ++i) {
                entries[i].end();
                if (entriesHisto != null) {
                    entriesHisto[i].end();
                }
            }
        }

        statsTiming.sample((System.nanoTime() - t0 + 500) / 1000);
    }

    private final ItemUpdateListener LISTENER = new ItemUpdateListener() {
        @Override
        public void handleItemUpdated(Item<?> item, long now, long appNow, int intervalIndex, long intervalMillis,
                String intervalName) {
            if (entries == null && intraday == StatsIntradayLogger.NULL) {
                return;
            }

            final Value v = item.getValue();
            final History history = v.getHistory();
            LogEntry e;

            final long n = history.getN(intervalIndex, 1);
            final long sum = history.getSum(intervalIndex, 1);
            final long last = history.getLast(intervalIndex, 1);
            final long min = history.getMin(intervalIndex, 1);
            final long max = history.getMax(intervalIndex, 1);
            final long avg = history.getAvg(intervalIndex, 1);
            final long sum2 = history.getSum2(intervalIndex, 1);
            final long stdev = history.getStdev(intervalIndex, 1);
            if (entries != null) {
                e = entries[intervalIndex];
                if (e.size() > BUFFER_SIZE - GUESS_ENTRY_SIZE) {
                    e.end();
                    e = entries[intervalIndex] = entryPool.take().start(sink, LogLevel.INFO, now * 1000);
                }

                e.append("STAT")
                        .append(',').append(intervalName)
                        .append(',').append(now / 1000)
                        .append(',').appendTimestamp(now, systemTimestamp)
                        .append(',').append(appNow / 1000)
                        .append(',').appendTimestamp(appNow, appTimestamp)
                        .append(',').append(v.getTypeTag())
                        .append(',').append(item.getGroupName())
                        .append('.').append(item.getName())
                        .append(',').append(n)
                        .append(',').append(sum)
                        .append(',').append(last)
                        .append(',').append(min)
                        .append(',').append(max)
                        .append(',').append(avg)
                        .append(',').append(sum2)
                        .append(',').append(stdev)
                        .nl();
            }
            intraday.log(
                    intervalName,
                    now,
                    appNow,
                    v.getTypeTag(),
                    item.getCompactName(),
                    n,
                    sum,
                    last,
                    min,
                    max,
                    avg,
                    sum2,
                    stdev);
        }
    };
}

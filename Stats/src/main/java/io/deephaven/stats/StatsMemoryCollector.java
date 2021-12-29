/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stats;

import io.deephaven.base.clock.TimeConstants;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.State;
import io.deephaven.hash.KeyedObjectHash;
import io.deephaven.hash.KeyedObjectKey;

import java.lang.management.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class StatsMemoryCollector {
    private static final long NANOS = 1000000000;
    private static final long MICROS = 1000000;
    private static final long MILLIS = 1000;

    private final MemoryMXBean memoryBean;
    private final Consumer<String> alertFunction;
    private final BooleanSupplier cmsAlertEnabled;

    /*
     * This used to use the ServerStatus.getJvmUptime(), which is really only interesting because it is the first time
     * that the ServerStatus had an opportunity to call System.currentTimeMillis(). Because the StatsDriver is generally
     * created, we'll just make our own uptime calcuation.
     */
    private static final long statsStartupTime = System.currentTimeMillis();

    private static long getStatsUptime() {
        return System.currentTimeMillis() - statsStartupTime;
    }

    private static class PoolState {
        private final long seconds;
        private final MemoryPoolMXBean bean;
        private final Value used;
        private final Value committed;
        private final Value max;

        public PoolState(long seconds, MemoryPoolMXBean bean) {
            this.seconds = seconds;
            this.bean = bean;
            this.used = Stats.makeItem("Memory-Pool", bean.getName() + "-Used", State.FACTORY).getValue();
            this.committed = Stats.makeItem("Memory-Pool", bean.getName() + "-Committed", State.FACTORY).getValue();
            this.max = Stats.makeItem("Memory-Pool", bean.getName() + "-Max", State.FACTORY).getValue();
        }

        public static KeyedObjectKey<String, PoolState> keyDef = new KeyedObjectKey<String, PoolState>() {
            public String getKey(PoolState v) {
                return v.bean.getName();
            }

            public int hashKey(String k) {
                return k.hashCode();
            }

            public boolean equalKey(String k, PoolState v) {
                return k.equals(v.bean.getName());
            }
        };

        public void update() {
            MemoryUsage u = bean.getUsage();
            used.sample(u.getUsed());
            max.sample(u.getMax());
            committed.sample(u.getCommitted());
        }
    }

    private final KeyedObjectHash<String, PoolState> pools = new KeyedObjectHash<String, PoolState>(PoolState.keyDef);

    private static class CollectorState {
        private final long seconds;
        private final GarbageCollectorMXBean bean;
        private final Consumer<String> alertFunction;
        private BooleanSupplier enableCmsAlerts;
        private final Value count;
        private final Value time;
        private long lastCount = -1;
        private long lastTime = -1;
        private long lastCMSOccurrence = 0;
        private long lastCMSOccurrenceMailSent = 0;

        public CollectorState(long seconds, GarbageCollectorMXBean bean, Consumer<String> alertFunction,
                BooleanSupplier enableCmsAlerts) {
            this.seconds = seconds;
            this.bean = bean;
            this.alertFunction = alertFunction;
            this.enableCmsAlerts = enableCmsAlerts;
            this.count = Stats.makeItem("Memory-GC", bean.getName() + "-Count", Counter.FACTORY).getValue();
            this.time = Stats.makeItem("Memory-GC", bean.getName() + "-Time", Counter.FACTORY).getValue();
        }

        public static KeyedObjectKey<String, CollectorState> keyDef = new KeyedObjectKey<String, CollectorState>() {
            public String getKey(CollectorState v) {
                return v.bean.getName();
            }

            public int hashKey(String k) {
                return k.hashCode();
            }

            public boolean equalKey(String k, CollectorState v) {
                return k.equals(v.bean.getName());
            }
        };

        public void update() {
            long c = bean.getCollectionCount();
            long t = bean.getCollectionTime();
            if (lastCount != -1) {
                count.sample(c - lastCount);

                long timeSample = (t - lastTime) / seconds;

                time.sample(timeSample);

                if ("ConcurrentMarkSweep".equals(bean.getName())) {
                    if (c - lastCount > 0 && getStatsUptime() > 600000) {
                        final long now = System.currentTimeMillis();
                        if (now - lastCMSOccurrence < 30000) { // twice in 30 seconds seems like a bit much
                            if (now - lastCMSOccurrenceMailSent > TimeConstants.HOUR) { // send at most one an hour
                                SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
                                final String message =
                                        "Last CMS collection at " + dateFormat.format(new Date(lastCMSOccurrence));
                                if (alertFunction != null) {
                                    alertFunction.accept("Rapid CMS detected, " + message);
                                }
                                lastCMSOccurrenceMailSent = now;
                            }
                        }
                        lastCMSOccurrence = now;
                    }
                }
            }
            lastCount = c;
            lastTime = t;
        }
    }

    private final KeyedObjectHash<String, CollectorState> collectors =
            new KeyedObjectHash<String, CollectorState>(CollectorState.keyDef);

    private final long seconds;

    private final Value heapUsed;
    private final Value heapCommitted;
    private final Value heapMax;

    private final Value nonHeapUsed;
    private final Value nonHeapCommitted;
    private final Value nonHeapMax;

    StatsMemoryCollector(long interval, Consumer<String> alertFunction, BooleanSupplier cmsAlertEnabled) {
        this.alertFunction = alertFunction;
        this.cmsAlertEnabled = cmsAlertEnabled;
        this.seconds = interval / MILLIS;
        memoryBean = ManagementFactory.getMemoryMXBean();

        this.heapUsed = Stats.makeItem("Memory-Heap", "Used", State.FACTORY).getValue();
        this.heapCommitted = Stats.makeItem("Memory-Heap", "Committed", State.FACTORY).getValue();
        this.heapMax = Stats.makeItem("Memory-Heap", "Max", State.FACTORY).getValue();

        this.nonHeapUsed = Stats.makeItem("Memory-NonHeap", "Used", State.FACTORY).getValue();
        this.nonHeapCommitted = Stats.makeItem("Memory-NonHeap", "Committed", State.FACTORY).getValue();
        this.nonHeapMax = Stats.makeItem("Memory-NonHeap", "Max", State.FACTORY).getValue();
    }

    /**
     * At each invocation, measure all thread's CPU usage.
     */
    public void update() {
        MemoryUsage heap = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();

        heapUsed.sample(heap.getUsed());
        heapCommitted.sample(heap.getCommitted());
        heapMax.sample(heap.getMax());

        nonHeapUsed.sample(nonHeap.getUsed());
        nonHeapCommitted.sample(nonHeap.getCommitted());
        nonHeapMax.sample(nonHeap.getMax());

        for (MemoryPoolMXBean b : ManagementFactory.getMemoryPoolMXBeans()) {
            PoolState pool = pools.get(b.getName());
            if (pool == null) {
                pools.add(pool = new PoolState(seconds, b));
            }
            pool.update();
        }

        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            CollectorState collector = collectors.get(b.getName());
            if (collector == null) {
                collectors.add(collector = new CollectorState(seconds, b, alertFunction, cmsAlertEnabled));
            }
            collector.update();
        }
    }
}

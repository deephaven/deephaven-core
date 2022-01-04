package io.deephaven.benchmarking.runner;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class EnvUtils {
    private static List<GarbageCollectorMXBean> gcBeans = null;

    public static class GcTimeCollector {
        private enum State {
            STARTED, STOPPED,
        }

        /**
         * In STARTED state, absolute count of collections since program start. In STOPPED state, number of collections
         * between last call to {@code resetAndStart} and last call to {@code stopAndSample}.
         */
        private long collectionCount;
        /**
         * In STARTED state, absolute time in millis in collections since program start. In STOPPED state, time in
         * millis for collections between last call to {@code resetAndStart} and last call to {@code stopAndSample}.
         */
        private long collectionTimeMs;
        private State state;

        public GcTimeCollector() {
            state = State.STOPPED;
        }

        public void resetAndStart() {
            if (state != State.STOPPED) {
                throw new IllegalStateException("Already started");
            }
            synchronized (EnvUtils.class) {
                if (gcBeans == null) {
                    gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
                }
            }
            long countAccum = 0;
            long timeAccumMs = 0;
            for (final GarbageCollectorMXBean gcBean : gcBeans) {
                countAccum += gcBean.getCollectionCount();
                timeAccumMs += gcBean.getCollectionTime();
            }
            collectionCount = countAccum;
            collectionTimeMs = timeAccumMs;
            state = State.STARTED;
        }

        public void stopAndSample() {
            if (state != State.STARTED) {
                throw new IllegalStateException("Not started");
            }
            long countAccum = 0;
            long timeAccumMs = 0;
            for (final GarbageCollectorMXBean gcBean : gcBeans) {
                countAccum += gcBean.getCollectionCount();
                timeAccumMs += gcBean.getCollectionTime();
            }
            collectionCount = countAccum - collectionCount;
            collectionTimeMs = timeAccumMs - collectionTimeMs;
            state = State.STOPPED;
        }

        public long getCollectionCount() {
            if (state != State.STOPPED) {
                throw new IllegalStateException("Still running");
            }
            return collectionCount;
        }

        public long getCollectionTimeMs() {
            if (state != State.STOPPED) {
                throw new IllegalStateException("Still running");
            }
            return collectionTimeMs;
        }
    }
}


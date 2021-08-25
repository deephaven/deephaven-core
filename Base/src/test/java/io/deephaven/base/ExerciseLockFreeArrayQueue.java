/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Random;

public class ExerciseLockFreeArrayQueue {

    public static void error(String s) {
        System.err.println(s);
    }

    public static void die(String s) {
        error(s);
        System.exit(1);
    }

    public static long work(Random r, int min, int max) {
        long work_sum = 0;
        int n = min + r.nextInt(Math.max(0, max - min));
        for (int work = n; work > 0; --work) {
            work_sum += (r.nextInt(2) & 1);
        }
        return work_sum * 2;
    }

    // ------------------------------------------------------------------------------------------------
    // the objects used in the test - like Integer, but no autoboxing or interned instances
    // ------------------------------------------------------------------------------------------------

    public static final class Int {
        final int val;

        Int(int val) {
            this.val = val;
        }

        public String toString() {
            return Integer.toString(val);
        }
    }

    public static final int MAX = 10000000;

    public static final Int[] obs = new Int[MAX];
    static {
        System.err.println("Allocating");
        for (int i = 0; i < obs.length; ++i) {
            obs[i] = new Int(i);
        }
    }

    // consumers can count the number of times they dequeue each object here
    public static final AtomicIntegerArray dequeueCounts = new AtomicIntegerArray(obs.length);

    public static void resetDequeueCounts() {
        for (int i = 0; i < obs.length; ++i) {
            dequeueCounts.set(i, 0);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // execution thread
    // ------------------------------------------------------------------------------------------------

    public static class BenchRunner {
        Runnable running = null;

        int id = -1;
        long put_count = 0;
        long get_count = 0;
        long put_work = 0;
        long get_work = 0;
        long put_spins = 0;
        long get_spins = 0;

        public BenchRunner() {
            Thread t = new Thread() {
                public void run() {
                    while (true) {
                        synchronized (BenchRunner.this) {
                            while (running == null) {
                                try {
                                    BenchRunner.this.wait();
                                } catch (InterruptedException x) {
                                    die(x.toString());
                                }
                            }
                            running.run();
                            running = null;
                            BenchRunner.this.notify();
                        }
                    }
                }
            };
            t.setDaemon(true);
            t.start();
        }

        public void start(Runnable r) {
            synchronized (this) {
                running = r;
                notify();
            }
        }

        public void finish() {
            synchronized (this) {
                while (running != null) {
                    try {
                        wait();
                    } catch (InterruptedException x) {
                        die(x.toString());
                    }
                }
            }
        }
    }

    static final BenchRunner[] runners = new BenchRunner[20];
    static {
        for (int i = 0; i < runners.length; ++i) {
            runners[i] = new BenchRunner();
        }
    }

    private static void resetRunners() {
        for (int i = 0; i < runners.length; ++i) {
            runners[i].id = -1;
            runners[i].put_count = 0;
            runners[i].get_count = 0;
            runners[i].put_work = 0;
            runners[i].get_work = 0;
            runners[i].put_spins = 0;
            runners[i].get_spins = 0;
        }
    }

    private static void dumpRunnerState(String s) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append(s).append(": ");
        appendRunnerState(sb);
        System.err.println(sb);
    }

    private static String runnerState() {
        StringBuilder sb = new StringBuilder(1024);
        appendRunnerState(sb);
        return sb.toString();
    }

    private static void appendRunnerState(StringBuilder sb) {
        sb.append("Runners");
        for (int i = 0; i < runners.length; i++) {
            BenchRunner runner = runners[i];
            if (runner.id >= 0) {
                sb.append(" ");
                sb.append(runner.running == null ? "W" : "R");
                sb.append(runner.put_count).append('/').append(runner.put_spins);
                sb.append("!");
                sb.append(runner.get_count).append('/').append(runner.get_spins);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // abstract queue, for implementation comparison
    // ------------------------------------------------------------------------------------------------

    public interface BenchQueue<T> {
        void init();

        int cap();

        void put(T el, BenchRunner runner);

        T get(BenchRunner runner);

        int head();

        int tail();
    }

    public static final long ABORT_NANOS = 10000000000L;

    public static class LFAQ<T> implements BenchQueue<T> {
        public final LockFreeArrayQueue<T> q;

        public LFAQ(int cap) {
            int log2cap = 32 - Integer.numberOfLeadingZeros(cap - 1);
            q = new LockFreeArrayQueue<T>(log2cap);
        }

        public void init() {
            q.init();
        }

        public int cap() {
            return q.capacity();
        }

        public void put(T el, BenchRunner runner) {
            int spin = 0;
            long t0 = 0;
            long head0 = q.head.get();
            long tail0 = q.tail.get();
            while (!q.enqueue(el)) {
                ++spin;
                ++runner.put_spins;
                if (spin == 1000000) {
                    t0 = System.nanoTime();
                    Thread.yield();
                } else if (spin == 1000001) {
                    long dt = System.nanoTime() - t0;
                    error("TZ2: stuck producer " + runner.id + ": back after yield, dt=" + dt);
                } else if (spin > 1000000) {
                    if (spin % 1000000 == 0) {
                        long dt = System.nanoTime() - t0;
                        error("TZ2: stuck producer " + runner.id + ": " + spin + " spins on element " + el
                                + ", initial: " + head0 + "/" + tail0 + ", now: " + q.head.get() + "/" + q.tail.get()
                                + ", dt=" + (dt / 1000000) + " " + runnerState());
                        if (dt - t0 > ABORT_NANOS) {
                            error("TZ2: aborting producer " + runner.id + " after " + (dt / 1000000) + " millis");
                            break;
                        }
                    }
                }
            }
            runner.put_count++;
        }

        public T get(BenchRunner runner) {
            T v;
            int spin = 0;
            long t0 = 0;
            long head0 = q.head.get();
            long tail0 = q.tail.get();
            while ((v = q.dequeue()) == null) {
                ++spin;
                ++runner.get_spins;
                if (spin == 1000000) {
                    t0 = System.nanoTime();
                    Thread.yield();
                } else if (spin == 1000001) {
                    long dt = System.nanoTime() - t0;
                    error("TZ2: stuck consumer " + runner.id + ": back after yield, dt=" + dt);
                } else if (spin % 1000000 == 0) {
                    long dt = System.nanoTime() - t0;
                    error("TZ2: stuck consumer " + runner.id + ": " + spin + " spins"
                            + ", initial: " + head0 + "/" + tail0 + ", now: " + q.head.get() + "/" + q.tail.get()
                            + ", dt=" + (dt / 1000000) + " " + runnerState());
                    if (dt > ABORT_NANOS) {
                        error("TZ2: aborting consumer " + runner.id + " after " + (dt / 1000000) + " millis");
                        break;
                    }
                }
            }
            runner.get_count++;
            return v;
        }

        public int head() {
            return q.head.get();
        }

        public int tail() {
            return q.tail.get();
        }
    }

    public static class ABQ<T> implements BenchQueue<T> {
        public final int cap;
        public final ArrayBlockingQueue<T> q;

        public ABQ(int cap) {
            this.cap = cap;
            q = new ArrayBlockingQueue<T>(cap);
        }

        public void init() {
            q.clear();
        }

        public int cap() {
            return cap;
        }

        public void put(T el, BenchRunner runner) {
            try {
                q.put(el);
            } catch (InterruptedException x) {
                error(x.toString());
            }
            runner.put_spins += 0;
            runner.put_count++;
        }

        public T get(BenchRunner runner) {
            try {
                runner.get_spins += 0;
                runner.get_count++;
                return q.take();
            } catch (InterruptedException x) {
                error(x.toString());
                return null;
            }
        }

        public int head() {
            return 0;
        }

        public int tail() {
            return 0;
        }
    }

    public static class PBQ<T> implements BenchQueue<T> {
        public int cap;
        public final ArrayBlockingQueue<T> q;

        public PBQ(int cap) {
            this.cap = cap;
            q = new ArrayBlockingQueue<T>(cap);
        }

        public void init() {
            q.clear();
        }

        public int cap() {
            return cap;
        }

        public void put(T el, BenchRunner runner) {
            int spin = 0;
            long t0 = 0;
            while (!q.offer(el)) {
                ++spin;
                if (spin == 1000000) {
                    t0 = System.nanoTime();
                } else if (spin % 1000000 == 0 && System.nanoTime() - t0 > ABORT_NANOS) {
                    error("PBQ: aborting stuck producer after " + (ABORT_NANOS / 1000000) + " millis and " + spin
                            + " spins on element " + el);
                    break;
                }
            }
            runner.put_spins += spin;
            runner.put_count++;
        }

        public T get(BenchRunner runner) {
            T v;
            int spin = 0;
            long t0 = 0;
            while ((v = q.poll()) == null) {
                ++spin;
                if (spin == 1000000) {
                    t0 = System.nanoTime();
                } else if (spin % 1000000 == 0 && System.nanoTime() - t0 > ABORT_NANOS) {
                    error("PBQ: aborting stuck consumer after " + (ABORT_NANOS / 1000000) + " millis and " + spin
                            + " spins");
                    break;
                }
            }
            runner.get_spins += spin;
            runner.get_count++;
            return v;
        }

        public int head() {
            return 0;
        }

        public int tail() {
            return 0;
        }
    }

    public static class RBQ<T> implements BenchQueue<T> {
        public final int cap;
        public final RingBuffer<T> q;

        public RBQ(int cap) {
            this.cap = cap;
            q = new RingBuffer<T>(cap + 1);
        }

        public void init() {
            q.clear();
        }

        public int cap() {
            return cap;
        }

        public void put(T el, BenchRunner runner) {
            synchronized (q) {
                while (q.size() == cap) {
                    try {
                        q.wait();
                    } catch (InterruptedException x) {
                        die(x.toString());
                    }
                }
                q.add(el);
                q.notify();
            }
            runner.put_spins += 0;
            runner.put_count++;
        }

        public T get(BenchRunner runner) {
            synchronized (q) {
                while (q.size() == 0) {
                    try {
                        q.wait();
                    } catch (InterruptedException x) {
                        die(x.toString());
                        return null;
                    }
                }
                T val = q.remove();
                q.notify();
                runner.get_spins += 0;
                runner.get_count++;
                return val;
            }
        }

        public int head() {
            return 0;
        }

        public int tail() {
            return 0;
        }
    }

    // ------------------------------------------------------------------------------------------------
    // trivial test that just enqueues and dequeues in sequence
    // ------------------------------------------------------------------------------------------------

    public static void testSingleThreaded(final BenchQueue<Int> q) {
        q.init();
        resetRunners();

        runners[0].id = 0;
        runners[0].start(new Runnable() {
            public void run() {
                for (int i = 0; i < 10000; ++i) {
                    q.put(obs[i], runners[0]);
                    Object v = q.get(runners[0]);
                    if (v != obs[i]) {
                        die("expected " + i + ", got " + v);
                    }
                }
            }
        });
    }

    // ------------------------------------------------------------------------------------------------
    // one producer, one consumer; checks that objects are enqueued and dequeued in correct order
    // ------------------------------------------------------------------------------------------------

    public static AtomicBoolean testTwoThreadsFailed = new AtomicBoolean(false);

    public static void testOrdered(final int trial, final BenchQueue<Int> q, final int N,
            final int PRODUCER_MIN_WORK, final int PRODUCER_MAX_WORK,
            final int CONSUMER_MIN_WORK, final int CONSUMER_MAX_WORK) {
        q.init();
        resetRunners();

        runners[0].id = 0;
        runners[0].start(new Runnable() {
            public void run() {
                final Random r = new Random();
                for (int i = 0; i < N; ++i) {
                    q.put(obs[i], runners[0]);
                    if (PRODUCER_MAX_WORK > 0) {
                        runners[0].put_work += work(r, PRODUCER_MIN_WORK, PRODUCER_MAX_WORK);
                    }
                }
            }
        });

        runners[1].id = 0;
        runners[1].start(new Runnable() {
            public void run() {
                final Random r = new Random();
                for (int i = 0; i < N; ++i) {
                    Int v = q.get(runners[1]);
                    // note: test identity, not equality
                    if (v != obs[i]) {
                        error("Trial" + trial + ": expected " + i + ", got " + v);
                        testTwoThreadsFailed.set(true);
                        break;
                    }
                    if (CONSUMER_MAX_WORK > 0) {
                        runners[1].get_work += work(r, CONSUMER_MIN_WORK, CONSUMER_MAX_WORK);
                    }
                }
            }
        });

        long t0 = System.nanoTime();
        runners[0].finish();
        runners[1].finish();
        long t1 = System.nanoTime();

        System.out.println("testOrdered" + ","
                + q.getClass().getSimpleName() + ","
                + trial + ","
                + q.cap() + ","
                + N + ","
                + 1 + ","
                + 1 + ","
                + PRODUCER_MIN_WORK + ","
                + PRODUCER_MAX_WORK + ","
                + CONSUMER_MIN_WORK + ","
                + CONSUMER_MAX_WORK + ","
                + (runners[0].put_work / N) + ","
                + (runners[1].get_work / N) + ","
                + ((double) runners[0].put_spins / N) + ","
                + ((double) runners[1].get_spins / N) + ","
                + ((t1 - t0) / N) + ","
                + q.head() + ","
                + q.tail() + ","
                + (q.head() % q.cap()) + ","
                + (q.tail() % q.cap()) + ",");

        if (testTwoThreadsFailed.get()) {
            System.err.println("Trial " + trial + ": testOrdered: failed ");
        }
    }

    // ------------------------------------------------------------------------------------------------
    // multiple producers and consumers
    // ------------------------------------------------------------------------------------------------

    public static void testManyThreads(final int trial, final BenchQueue<Int> q,
            final int num_producers, final int num_consumers, final int N,
            final int PRODUCER_MIN_WORK, final int PRODUCER_MAX_WORK,
            final int CONSUMER_MIN_WORK, final int CONSUMER_MAX_WORK) {
        q.init();
        resetDequeueCounts();
        resetRunners();

        int runner_index = 0;
        long t0 = System.nanoTime();

        int p = 0;
        for (int i = 0; i < num_producers; ++i) {
            final int base = i;
            final BenchRunner runner = runners[runner_index++];
            runner.id = i;
            runner.start(new Runnable() {
                public void run() {
                    final Random r = new Random(314159);
                    for (int i = base; i < N; i += num_producers) {
                        q.put(obs[i], runner);
                        if (PRODUCER_MAX_WORK > 0) {
                            runner.put_work += work(r, PRODUCER_MIN_WORK, PRODUCER_MAX_WORK);
                        }
                    }
                }
            });
        }

        int c = 0;
        for (int i = 0; i < num_consumers; ++i) {
            final boolean last = (i == num_consumers - 1);
            final int my_N = last ? (N - c) : (N / num_consumers);
            c += my_N;
            final BenchRunner runner = runners[runner_index++];
            runner.id = i;
            runner.start(new Runnable() {
                public void run() {
                    final Random r = new Random(314159);
                    long total_spins = 0;
                    for (int i = 0; i < my_N; ++i) {
                        Int v = q.get(runner);
                        if (v != null) {
                            dequeueCounts.incrementAndGet(v.val);
                        }
                        if (CONSUMER_MAX_WORK > 0) {
                            runner.get_work += work(r, CONSUMER_MIN_WORK, CONSUMER_MAX_WORK);
                        }
                    }
                }
            });
        }

        for (int i = 0; i < runner_index; ++i) {
            runners[i].finish();
        }

        long t1 = System.nanoTime();

        runner_index = 0;
        long total_producer_spins = 0, total_consumer_spins = 0;
        long total_producer_work = 0, total_consumer_work = 0;
        for (int i = 0; i < num_producers; ++i) {
            total_producer_spins += runners[runner_index++].put_spins;
            total_producer_work += runners[runner_index++].put_work;
        }
        for (int i = 0; i < num_consumers; ++i) {
            total_consumer_spins += runners[runner_index++].get_spins;
            total_consumer_work += runners[runner_index++].get_work;
        }

        System.out.println("testManyThreads" + ","
                + q.getClass().getSimpleName() + ","
                + trial + ","
                + q.cap() + ","
                + N + ","
                + num_producers + ","
                + num_consumers + ","
                + PRODUCER_MIN_WORK + ","
                + PRODUCER_MAX_WORK + ","
                + CONSUMER_MIN_WORK + ","
                + CONSUMER_MAX_WORK + ","
                + (total_producer_work / N) + ","
                + (total_consumer_work / N) + ","
                + ((double) total_producer_spins / N) + ","
                + ((double) total_consumer_spins / N) + ","
                + ((t1 - t0) / N) + ","
                + q.head() + ","
                + q.tail() + ","
                + (q.head() % q.cap()) + ","
                + (q.tail() % q.cap()) + ",");

        for (int i = 0; i < N; ++i) {
            if (dequeueCounts.get(i) != 1) {
                error("Trial " + trial + " testManyThreads: object " + i + " was dequeued " + dequeueCounts.get(i)
                        + " times");
            }
        }
    }


    // ------------------------------------------------------------------------------------------------
    // test driver
    // ------------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        testSingleThreaded(new LFAQ<Int>(1000));

        final int[] caps = new int[] {
                100,
                1000,
                10000,
                10000000
        };

        System.err.println("Flushing young generation");
        for (int i = 0; i < 1000000; ++i) {
            int[] x = new int[250];
        }

        final int ITER = 1000;

        final int MIN_P = 1;
        final int MAX_P = 5;
        final int MIN_C = 1;
        final int MAX_C = 5;

        final int MIN_PW = 0;
        final int MAX_PW = 0;
        final int MIN_CW = 0;
        final int MAX_CW = 0;

        if (true) {
            for (int i = 0; i < ITER; ++i) {
                for (int pw = MIN_PW; pw <= MAX_PW; pw++) {
                    for (int cw = MIN_CW; cw <= MAX_CW; cw++) {
                        for (int p = MIN_P; p <= MAX_P; ++p) {
                            for (int c = MIN_C; c <= MAX_C; ++c) {
                                for (int cap = 0; cap < caps.length; ++cap) {
                                    BenchQueue q = new LFAQ<Int>(caps[cap]);
                                    // testTwoThreadsFailed.set(false);
                                    // testOrdered(i, q,
                                    // 10000000, // number of operations
                                    // 0, pw, // min, max producer work iterations between ops
                                    // 0, cw); // min, max consumer work iterations between ops

                                    testManyThreads(i, q,
                                            p, c, // number of producers/consumers
                                            10000000, // number of operations
                                            0, pw * 20, // min, max producer work iterations between ops
                                            0, cw * 20); // min, max consumer work iterations between ops
                                }
                            }
                        }
                    }
                }
            }
            System.exit(1);
        }
    }
}

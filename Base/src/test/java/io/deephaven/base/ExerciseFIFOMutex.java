/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

public class ExerciseFIFOMutex {

    private volatile boolean stop = false;
    private final FIFOMutex mutex = new FIFOMutex();
    private int foo = 0;

    private class Locker implements Runnable {
        final int id;
        int loopCount = 0;

        public Locker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            while (!stop) {
                mutex.lock();
                for (int i = 0; i < 100; ++i) {
                    foo++;
                }
                mutex.unlock();
                loopCount++;
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException x) {
            // ignore
        }

    }

    private void start(final int NTHREADS) {
        Locker[] lockers = new Locker[NTHREADS];
        Thread[] threads = new Thread[NTHREADS];
        for (int i = 0; i < NTHREADS; ++i) {
            lockers[i] = new Locker(i);
            threads[i] = new Thread(lockers[i]);
            threads[i].setName("Locker-" + i);
            threads[i].setDaemon(true);
        }

        // lock the mutex, then start the threads and wait for them to join the mutex queue
        mutex.lock();
        for (Thread t : threads) {
            t.start();
        }
        sleep(1000);

        // let 'er rip!
        mutex.unlock();
        sleep(5000);

        stop = true;
        int totalLoops = 0;
        int minLoops = Integer.MAX_VALUE, maxLoops = Integer.MIN_VALUE;
        for (int i = 0; i < NTHREADS; ++i) {
            Thread t = threads[i];
            try {
                t.join(1000);
                if (t.isAlive()) {
                    System.out.println("Couldn't stop thread " + i);
                } else {
                    // System.out.println("Locker "+i+"/"+NTHREADS+" executed "+lockers[i].loopCount+" loops");
                    totalLoops += lockers[i].loopCount;
                    minLoops = Math.min(minLoops, lockers[i].loopCount);
                    maxLoops = Math.max(maxLoops, lockers[i].loopCount);
                }
            } catch (InterruptedException x) {
                // ignore
            }
        }
        System.out.println(NTHREADS + " lockers executed " + totalLoops + " loops in total; min=" + minLoops + ", max="
                + maxLoops);
    }

    public static void main(String[] args) {
        new ExerciseFIFOMutex().start(1);
        new ExerciseFIFOMutex().start(2);
        new ExerciseFIFOMutex().start(3);
        new ExerciseFIFOMutex().start(4);
        new ExerciseFIFOMutex().start(5);
        new ExerciseFIFOMutex().start(6);
        new ExerciseFIFOMutex().start(7);
        new ExerciseFIFOMutex().start(8);
        new ExerciseFIFOMutex().start(9);
    }
}

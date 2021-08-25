/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.clock;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.LoggerTimeSource;

public class MicroTimer {

    private static final boolean isNative = Configuration.getInstance().getBoolean("NIO.wireLagClock.native");

    private static long nanoTimeOffset = System.currentTimeMillis() * 1000000 - System.nanoTime();

    static {
        if (isNative) {
            System.loadLibrary("FishCommon");
        }
    }

    public static long currentTimeMicros() {
        return isNative ? currentTimeMicrosNative() : (System.nanoTime() + nanoTimeOffset) / 1000;
    }

    public static long clockRealtime() {
        return isNative ? clockRealtimeNative() : (System.nanoTime() + nanoTimeOffset);
    }


    public static long clockMonotonic() {
        return isNative ? clockMonotonicNative() : System.nanoTime();
    }

    public static long rdtsc() {
        return rdtscNative();
    }

    public static native long currentTimeMicrosNative();

    public static native long clockRealtimeNative();

    public static native long clockMonotonicNative();

    public static native long rdtscNative();

    private static LoggerTimeSource staticMicrostampTimeSource = new LoggerTimeSource() {
        @Override
        public long currentTimeMicros() {
            return MicroTimer.currentTimeMicros();
        }
    };

    public static LoggerTimeSource getLoggerTimeSource() {
        return staticMicrostampTimeSource;
    }

    public static void main(String[] args) {
        long startNanos = System.nanoTime();
        long cycles = rdtscNative();

        int COUNT = 100000000;

        for (int i = 0; i < COUNT; i++) {
            rdtscNative();
        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("rdtscNative: " + (System.nanoTime() - startNanos) / COUNT + " nanos per");
        System.out.println("rdtscNative: " + (rdtscNative() - cycles) / COUNT + " cycles per");

        startNanos = System.nanoTime();
        cycles = rdtscNative();

        for (int i = 0; i < COUNT; i++) {
            currentTimeMicrosNative();
        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("currentTimeMicrosNative: " + (System.nanoTime() - startNanos) / COUNT + " nanos per");
        System.out.println("currentTimeMicrosNative: " + (rdtscNative() - cycles) / COUNT + " cycles per");

        startNanos = System.nanoTime();
        cycles = rdtscNative();

        for (int i = 0; i < COUNT; i++) {
            System.nanoTime();
        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("System.nanoTime: " + (System.nanoTime() - startNanos) / COUNT + " nanos per");
        System.out.println("System.nanoTime: " + (rdtscNative() - cycles) / COUNT + " cycles per");

        startNanos = System.nanoTime();
        cycles = rdtscNative();

        for (int i = 0; i < COUNT; i++) {
            System.currentTimeMillis();
        }

        System.out.println("-----------------------------------------------------------------------");
        System.out.println("System.currentTimeMillis: " + (System.nanoTime() - startNanos) / COUNT + " nanos per");
        System.out.println("System.currentTimeMillis: " + (rdtscNative() - cycles) / COUNT + " cycles per");
    }
}

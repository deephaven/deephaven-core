/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AtomicUtil {

    public static long compareAndSetAndGetOld(AtomicLong i, long expected, long updated) {
        long value;
        while ((value = i.get()) == expected) {
            if (i.compareAndSet(expected, updated)) {
                return expected;
            }
        }
        return value;
    }

    public static long getAndSetIfIncreases(AtomicLong i, long value) {
        return getAndSetIfIncreasesBy(i, 1, value);
    }

    // by >= 1
    public static long getAndSetIfIncreasesBy(AtomicLong i, long by, long value) {
        long current;
        while (value >= (current = i.get()) + by) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
        }
        return current;
    }

    public static long getAndSetIfDecreases(AtomicLong i, long value) {
        return getAndSetIfDecreases(i, 1, value);
    }

    public static long getAndSetIfDecreases(AtomicLong i, long by, long value) {
        long current;
        while (value <= (current = i.get()) - by) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
        }
        return current;
    }

    public static int getAndSetIfIncreases(AtomicInteger i, int value) {
        return getAndSetIfIncreasesBy(i, 1, value);
    }

    // by >= 1
    public static int getAndSetIfIncreasesBy(AtomicInteger i, int by, int value) {
        int current;
        while (value >= (current = i.get()) + by) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
        }
        return current;
    }

    public static boolean setIfLessThan(AtomicInteger i, int pivot, int value) {
        int current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, value)) {
                return true;
            }
            current = i.get();
        }
        return false;
    }

    public static boolean setIfGreaterThan(AtomicInteger i, int pivot, int value) {
        int current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, value)) {
                return true;
            }
            current = i.get();
        }
        return false;
    }

    public static int changeAndGetIfLessThan(AtomicInteger i, int by, int pivot, int negative) {
        int current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current + by;
            }
            current = i.get();
        }
        return negative;
    }

    // by must be positive!
    public static int getAndIncreaseIfLessThan(AtomicInteger i, int by, int pivot) {
        int current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    // by must be positive!
    public static int getAndDecreaseIfGreaterThan(AtomicInteger i, int by, int pivot) {
        int current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, current - by)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static int getAndSetIfGreaterThan(AtomicInteger i, int value, int pivot) {
        int current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static int getAndSetIfLessThan(AtomicInteger i, int value, int pivot) {
        int current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static int changeAndGetIfGreaterThan(AtomicInteger i, int by, int pivot, int negative) {
        int current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current + by;
            }
            current = i.get();
        }
        return negative;
    }

    public static boolean setIfLessThan(AtomicLong i, long pivot, long value) {
        long current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, value)) {
                return true;
            }
            current = i.get();
        }
        return false;
    }

    public static boolean setIfGreaterThan(AtomicLong i, long pivot, long value) {
        long current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, value)) {
                return true;
            }
            current = i.get();
        }
        return false;
    }

    public static long changeAndGetIfLessThan(AtomicLong i, long by, long pivot, long negative) {
        long current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current + by;
            }
            current = i.get();
        }
        return negative;
    }

    public static long changeAndGetIfGreaterThan(AtomicLong i, long by, long pivot, long negative) {
        long current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current + by;
            }
            current = i.get();
        }
        return negative;
    }

    public static long getAndSetIfGreaterThan(AtomicLong i, long value, long pivot) {
        long current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static long getAndSetIfLessThan(AtomicLong i, long value, long pivot) {
        long current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, value)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static long getAndIncreaseIfLessThan(AtomicLong i, long by, long pivot) {
        long current = i.get();
        while (current < pivot) {
            if (i.compareAndSet(current, current + by)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    // by must be positive!
    public static long getAndDecreaseIfGreaterThan(AtomicLong i, long by, long pivot) {
        long current = i.get();
        while (current > pivot) {
            if (i.compareAndSet(current, current - by)) {
                return current;
            }
            current = i.get();
        }
        return current;
    }

    public static int atomicOr(AtomicInteger i, int mask) {
        int expect, update;
        do {
            expect = i.get();
            update = expect | mask;
        } while (!i.compareAndSet(expect, update));
        return update;
    }

    public static int atomicAnd(AtomicInteger i, int mask) {
        int expect, update;
        do {
            expect = i.get();
            update = expect & mask;
        } while (!i.compareAndSet(expect, update));
        return update;
    }

    public static int atomicAndNot(AtomicInteger i, int mask) {
        int expect, update;
        do {
            expect = i.get();
            update = expect & ~mask;
        } while (!i.compareAndSet(expect, update));
        return update;
    }
}

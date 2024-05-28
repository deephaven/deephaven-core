//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.mutable;

/**
 * Minimal mutable wrapper for a {@code long} value. Loosely based on
 * {@code org.apache.commons.lang3.mutable.MutableLong}, but without inheriting from {@link Number}, or providing any
 * overloads that accept {@code Number} or any boxed types.
 * <p>
 * Deliberately does not extend {@code Number}, does not implement {@code toString()}/{@code equals}/{@code hashcode()},
 * or implement {@code Comparable}.
 */
public class MutableLong {
    private long value;

    public MutableLong() {

    }

    public MutableLong(final long value) {
        this.value = value;
    }

    public long get() {
        return value;
    }

    public void set(long value) {
        this.value = value;
    }

    public void add(long addend) {
        this.value += addend;
    }

    public long addAndGet(long addend) {
        value += addend;
        return value;
    }

    public long getAndAdd(long addend) {
        long old = value;
        value += addend;
        return old;
    }

    public long getAndIncrement() {
        return value++;
    }

    public void increment() {
        value++;
    }

    public void decrement() {
        value--;
    }

    public long incrementAndGet() {
        return ++value;
    }

    public void subtract(final long subtrahend) {
        this.value -= subtrahend;
    }
}

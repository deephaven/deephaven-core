//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.mutable;

/**
 * Minimal mutable wrapper for an {@code int} value. Loosely based on
 * {@code org.apache.commons.lang3.mutable.MutableInt}, but without inheriting from {@link Number}, or providing any
 * overloads that accept {@code Number} or any boxed types.
 * <p>
 * Deliberately does not extend {@code Number}, does not implement {@code toString()}/{@code equals}/{@code hashcode()},
 * or implement {@code Comparable}.
 */

public class MutableInt {
    private int value;

    public MutableInt() {

    }

    public MutableInt(final int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }

    public void set(int value) {
        this.value = value;
    }

    public void add(int addend) {
        this.value += addend;
    }

    public int addAndGet(int addend) {
        value += addend;
        return value;
    }

    public int getAndAdd(int addend) {
        int old = value;
        value += addend;
        return old;
    }

    public int getAndIncrement() {
        return value++;
    }

    public void increment() {
        value++;
    }

    public void decrement() {
        value--;
    }

    public int incrementAndGet() {
        return ++value;
    }

    public void subtract(final int subtrahend) {
        this.value -= subtrahend;
    }
}

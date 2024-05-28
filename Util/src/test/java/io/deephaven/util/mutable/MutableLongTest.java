//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.mutable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MutableLongTest {

    @Test
    public void longValue() {
        assertEquals(123, new MutableLong(123).get());
    }

    @Test
    public void setValue() {
        MutableLong v = new MutableLong(321);
        v.set(999);
        assertEquals(999, v.get());
    }

    @Test
    public void add() {
        MutableLong v = new MutableLong(1000);
        v.add(10);
        assertEquals(1010, v.get());
    }

    @Test
    public void addAndGet() {
        MutableLong v = new MutableLong(1000);
        long result = v.addAndGet(10);
        assertEquals(1010, result);
        assertEquals(1010, v.get());
    }

    @Test
    public void getAndAdd() {
        MutableLong v = new MutableLong(1000);
        long result = v.getAndAdd(10);
        assertEquals(1000, result);
        assertEquals(1010, v.get());
    }

    @Test
    public void getAndIncrement() {
        MutableLong v = new MutableLong(1000);
        long result = v.getAndIncrement();
        assertEquals(1000, result);
        assertEquals(1001, v.get());
    }

    @Test
    public void increment() {
        MutableLong v = new MutableLong(1000);
        v.increment();
        assertEquals(1001, v.get());
    }

    @Test
    public void decrement() {
        MutableLong v = new MutableLong(1000);
        v.decrement();
        assertEquals(999, v.get());
    }

    @Test
    public void incrementAndGet() {
        MutableLong v = new MutableLong(1000);
        long result = v.incrementAndGet();
        assertEquals(1001, result);
        assertEquals(1001, v.get());
    }

    @Test
    public void subtract() {
        MutableLong v = new MutableLong(1000);
        v.subtract(10);
        assertEquals(990, v.get());
    }
}

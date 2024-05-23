//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.mutable;

import org.junit.Test;

import static org.junit.Assert.*;

public class MutableIntTest {

    @Test
    public void intValue() {
        assertEquals(123, new MutableInt(123).intValue());
    }

    @Test
    public void setValue() {
        MutableInt v = new MutableInt(321);
        v.setValue(999);
        assertEquals(999, v.intValue());
    }

    @Test
    public void add() {
        MutableInt v = new MutableInt(1000);
        v.add(10);
        assertEquals(1010, v.intValue());
    }

    @Test
    public void addAndGet() {
        MutableInt v = new MutableInt(1000);
        int result = v.addAndGet(10);
        assertEquals(1010, result);
        assertEquals(1010, v.intValue());
    }

    @Test
    public void getAndAdd() {
        MutableInt v = new MutableInt(1000);
        int result = v.getAndAdd(10);
        assertEquals(1000, result);
        assertEquals(1010, v.intValue());
    }

    @Test
    public void getAndIncrement() {
        MutableInt v = new MutableInt(1000);
        int result = v.getAndIncrement();
        assertEquals(1000, result);
        assertEquals(1001, v.intValue());
    }

    @Test
    public void increment() {
        MutableInt v = new MutableInt(1000);
        v.increment();
        assertEquals(1001, v.intValue());
    }

    @Test
    public void decrement() {
        MutableInt v = new MutableInt(1000);
        v.decrement();
        assertEquals(999, v.intValue());
    }

    @Test
    public void incrementAndGet() {
        MutableInt v = new MutableInt(1000);
        int result = v.incrementAndGet();
        assertEquals(1001, result);
        assertEquals(1001, v.intValue());
    }

    @Test
    public void subtract() {
        MutableInt v = new MutableInt(1000);
        v.subtract(10);
        assertEquals(990, v.intValue());
    }
}

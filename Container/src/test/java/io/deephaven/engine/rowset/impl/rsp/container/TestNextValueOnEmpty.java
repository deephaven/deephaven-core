//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * {@link Container#nextValue} is documented to answer −1 when no value at or above the bound exists. An empty container
 * never has one, so that is the answer it owes for any bound.
 */
public class TestNextValueOnEmpty {

    @Test
    public void testEmptyContainerHasNoNextValue() {
        for (final Container c : new Container[] {
                Container.empty(),
                new RunContainer(),
                new ArrayContainer(4),
                new BitmapContainer(),
        }) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": holds nothing", 0, c.getCardinality());
            for (final int from : new int[] {0, 1, 32768, 65535}) {
                assertEquals(name + ": nextValue(" + from + ")", -1, c.nextValue((short) from));
            }
        }
    }

    /** With values present, the answer is the first one at or above the bound. */
    @Test
    public void testNextValueWithValuesPresent() {
        final ArrayContainer array = new ArrayContainer(4);
        array.iset((short) 10);
        array.iset((short) 20);
        for (final Container c : new Container[] {
                array,
                new RunContainer(10, 11).iset((short) 20),
                new BitmapContainer().iset((short) 10).iset((short) 20),
                Container.twoValues((short) 10, (short) 20),
        }) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": at the first value", 10, c.nextValue((short) 10));
            assertEquals(name + ": below the first value", 10, c.nextValue((short) 0));
            assertEquals(name + ": between the values", 20, c.nextValue((short) 11));
            assertEquals(name + ": above the last value", -1, c.nextValue((short) 21));
        }
    }
}

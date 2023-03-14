/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.vector;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.*;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Unit tests for {@link CharVectorColumnWrapper}.
 */
public class ObjectVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        final ObjectVector<?> vector = new ObjectVectorColumnWrapper<>(
                ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new String[] {"a", "b", "c"}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals("a", vector.get(0));
        assertEquals("b", vector.get(1));
        assertEquals("c", vector.get(2));
        assertNull(vector.get(3));
        assertNull(vector.get(-1));
        assertEquals(List.of("a", "b", "c"), List.of(vector.toArray()));
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(List.of(), List.of(vector.subVector(0, 0).toArray()));
        assertNull(vector.subVector(0, 0).get(0));
        assertNull(vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        assertEquals(List.of("a"), List.of(vector.subVector(0, 1).toArray()));
        assertNull(vector.subVector(0, 1).get(1));
        assertNull(vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        assertEquals(List.of("b"), List.of(vector.subVector(1, 2).toArray()));
        assertNull(vector.subVector(0, 1).get(1));
        assertNull(vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        assertEquals(List.of("b", "c"), List.of(vector.subVector(1, 3).toArray()));
        assertNull(vector.subVector(1, 3).get(2));
        assertNull(vector.subVector(0, 1).get(-1));
    }

    public void testSubVectorByPositions() {
        final IntegerArraySource integerArraySource = new IntegerArraySource();
        integerArraySource.ensureCapacity(6);
        for (int ii = 0; ii < 6; ++ii) {
            integerArraySource.set(ii, (ii + 1) * 10);
        }
        final ObjectVector<Integer> columnVector =
                new ObjectVectorColumnWrapper<>(integerArraySource, RowSetFactory.fromRange(0, 5));
        final IntVector intVectorDirect = new IntVectorDirect(10, 20, 30, 40, 50, 60);

        final Random random = new Random(42);

        for (int step = 0; step < 50; ++step) {
            ArrayList<Integer> expected = new ArrayList<>();
            TLongList positions = new TLongArrayList();
            for (int ii = 0; ii < 6; ++ii) {
                if (random.nextBoolean()) {
                    expected.add(((ii + 1) * 10));
                    positions.add(ii);
                }
            }

            final ObjectVector<Integer> columnResult =
                    columnVector.subVectorByPositions(positions.toArray(new long[positions.size()]));
            final IntVector directResult =
                    intVectorDirect.subVectorByPositions(positions.toArray(new long[positions.size()]));

            assertEquals(expected.size(), columnResult.size());
            assertEquals(expected.size(), directResult.size());

            for (int ii = 0; ii < expected.size(); ++ii) {
                assertEquals(expected.get(ii), columnResult.get(ii));
                assertEquals((int) expected.get(ii), directResult.get(ii));
            }
        }
    }

    /**
     * Verify that a ObjectVectorColumnWrapper can correctly invoke the 'getDirect' operation even when one of the
     * column sources is null.
     */
    public void testGetDirect() {
        final ObjectVectorDirect<?> vectorDirect = new ObjectVectorDirect<>("a", "b", "c");
        final ObjectVectorColumnWrapper<?> vector = new ObjectVectorColumnWrapper<>(
                ArrayBackedColumnSource.getMemoryColumnSourceUntyped(new Vector[] {vectorDirect, null}),
                RowSetFactory.fromRange(0, 1));
        final Vector<?> base = vector.getDirect();
        assertEquals(2, base.intSize());
        assertTrue(ObjectVectorDirect.class.isAssignableFrom(base.getClass()));
        assertNull(((ObjectVectorDirect<?>) base).get(1));
    }
}

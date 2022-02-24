/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.LongVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Unit tests for {@link LongVectorColumnWrapper}.
 */
public class LongVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        LongVector vector = new LongVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new long[]{(long)10, (long)20, (long)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((long)10, vector.get(0));
        assertEquals((long)20, vector.get(1));
        assertEquals((long)30, vector.get(2));
        assertEquals(NULL_LONG, vector.get(3));
        assertEquals(NULL_LONG, vector.get(-1));
        long[] longs = vector.toArray();
        assertEquals((long)10, longs[0]);
        assertEquals((long)20, longs[1]);
        assertEquals((long)30, longs[2]);
        assertEquals(3, longs.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_LONG, vector.subVector(0, 0).get(0));
        assertEquals(NULL_LONG, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        long[] longs3 = vector.subVector(0, 1).toArray();
        assertEquals(1,longs3.length);
        assertEquals((long)10,longs3[0]);

        assertEquals(NULL_LONG, vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        long[] longs1 = vector.subVector(1, 2).toArray();
        assertEquals(1,longs1.length);
        assertEquals((long)20,longs1[0]);
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(1));
        assertEquals(NULL_LONG, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        long[] longs2 = vector.subVector(1, 3).toArray();
        assertEquals(2,longs2.length);
        assertEquals((long)20,longs2[0]);
        assertEquals((long)30,longs2[1]);
        assertEquals(NULL_LONG,vector.subVector(1, 3).get(2));
        assertEquals(NULL_LONG,vector.subVector(0, 1).get(-1));
    }
}

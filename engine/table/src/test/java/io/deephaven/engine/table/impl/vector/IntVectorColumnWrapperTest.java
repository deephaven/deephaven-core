/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.IntVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Unit tests for {@link IntVectorColumnWrapper}.
 */
public class IntVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        IntVector vector = new IntVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new int[]{(int)10, (int)20, (int)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((int)10, vector.get(0));
        assertEquals((int)20, vector.get(1));
        assertEquals((int)30, vector.get(2));
        assertEquals(NULL_INT, vector.get(3));
        assertEquals(NULL_INT, vector.get(-1));
        int[] ints = vector.toArray();
        assertEquals((int)10, ints[0]);
        assertEquals((int)20, ints[1]);
        assertEquals((int)30, ints[2]);
        assertEquals(3, ints.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_INT, vector.subVector(0, 0).get(0));
        assertEquals(NULL_INT, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        int[] ints3 = vector.subVector(0, 1).toArray();
        assertEquals(1,ints3.length);
        assertEquals((int)10,ints3[0]);

        assertEquals(NULL_INT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_INT, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        int[] ints1 = vector.subVector(1, 2).toArray();
        assertEquals(1,ints1.length);
        assertEquals((int)20,ints1[0]);
        assertEquals(NULL_INT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_INT, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        int[] ints2 = vector.subVector(1, 3).toArray();
        assertEquals(2,ints2.length);
        assertEquals((int)20,ints2[0]);
        assertEquals((int)30,ints2[1]);
        assertEquals(NULL_INT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_INT,vector.subVector(0, 1).get(-1));
    }
}

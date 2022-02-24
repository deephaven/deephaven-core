/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.FloatVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * Unit tests for {@link FloatVectorColumnWrapper}.
 */
public class FloatVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        FloatVector vector = new FloatVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new float[]{(float)10, (float)20, (float)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((float)10, vector.get(0));
        assertEquals((float)20, vector.get(1));
        assertEquals((float)30, vector.get(2));
        assertEquals(NULL_FLOAT, vector.get(3));
        assertEquals(NULL_FLOAT, vector.get(-1));
        float[] floats = vector.toArray();
        assertEquals((float)10, floats[0]);
        assertEquals((float)20, floats[1]);
        assertEquals((float)30, floats[2]);
        assertEquals(3, floats.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_FLOAT, vector.subVector(0, 0).get(0));
        assertEquals(NULL_FLOAT, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        float[] floats3 = vector.subVector(0, 1).toArray();
        assertEquals(1,floats3.length);
        assertEquals((float)10,floats3[0]);

        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        float[] floats1 = vector.subVector(1, 2).toArray();
        assertEquals(1,floats1.length);
        assertEquals((float)20,floats1[0]);
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_FLOAT, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        float[] floats2 = vector.subVector(1, 3).toArray();
        assertEquals(2,floats2.length);
        assertEquals((float)20,floats2[0]);
        assertEquals((float)30,floats2[1]);
        assertEquals(NULL_FLOAT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_FLOAT,vector.subVector(0, 1).get(-1));
    }
}

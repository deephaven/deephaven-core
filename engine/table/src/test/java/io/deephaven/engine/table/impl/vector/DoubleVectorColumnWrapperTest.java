/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.DoubleVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Unit tests for {@link DoubleVectorColumnWrapper}.
 */
public class DoubleVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        DoubleVector vector = new DoubleVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new double[]{(double)10, (double)20, (double)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((double)10, vector.get(0));
        assertEquals((double)20, vector.get(1));
        assertEquals((double)30, vector.get(2));
        assertEquals(NULL_DOUBLE, vector.get(3));
        assertEquals(NULL_DOUBLE, vector.get(-1));
        double[] doubles = vector.toArray();
        assertEquals((double)10, doubles[0]);
        assertEquals((double)20, doubles[1]);
        assertEquals((double)30, doubles[2]);
        assertEquals(3, doubles.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_DOUBLE, vector.subVector(0, 0).get(0));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        double[] doubles3 = vector.subVector(0, 1).toArray();
        assertEquals(1,doubles3.length);
        assertEquals((double)10,doubles3[0]);

        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        double[] doubles1 = vector.subVector(1, 2).toArray();
        assertEquals(1,doubles1.length);
        assertEquals((double)20,doubles1[0]);
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_DOUBLE, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        double[] doubles2 = vector.subVector(1, 3).toArray();
        assertEquals(2,doubles2.length);
        assertEquals((double)20,doubles2[0]);
        assertEquals((double)30,doubles2[1]);
        assertEquals(NULL_DOUBLE,vector.subVector(1, 3).get(2));
        assertEquals(NULL_DOUBLE,vector.subVector(0, 1).get(-1));
    }
}

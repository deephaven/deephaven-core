/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.ShortVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * Unit tests for {@link ShortVectorColumnWrapper}.
 */
public class ShortVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        ShortVector vector = new ShortVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new short[]{(short)10, (short)20, (short)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((short)10, vector.get(0));
        assertEquals((short)20, vector.get(1));
        assertEquals((short)30, vector.get(2));
        assertEquals(NULL_SHORT, vector.get(3));
        assertEquals(NULL_SHORT, vector.get(-1));
        short[] shorts = vector.toArray();
        assertEquals((short)10, shorts[0]);
        assertEquals((short)20, shorts[1]);
        assertEquals((short)30, shorts[2]);
        assertEquals(3, shorts.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_SHORT, vector.subVector(0, 0).get(0));
        assertEquals(NULL_SHORT, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        short[] shorts3 = vector.subVector(0, 1).toArray();
        assertEquals(1,shorts3.length);
        assertEquals((short)10,shorts3[0]);

        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        short[] shorts1 = vector.subVector(1, 2).toArray();
        assertEquals(1,shorts1.length);
        assertEquals((short)20,shorts1[0]);
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(1));
        assertEquals(NULL_SHORT, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        short[] shorts2 = vector.subVector(1, 3).toArray();
        assertEquals(2,shorts2.length);
        assertEquals((short)20,shorts2[0]);
        assertEquals((short)30,shorts2[1]);
        assertEquals(NULL_SHORT,vector.subVector(1, 3).get(2));
        assertEquals(NULL_SHORT,vector.subVector(0, 1).get(-1));
    }
}

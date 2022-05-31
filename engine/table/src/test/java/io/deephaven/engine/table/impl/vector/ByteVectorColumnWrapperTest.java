/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorColumnWrapperTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.ByteVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * Unit tests for {@link ByteVectorColumnWrapper}.
 */
public class ByteVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        ByteVector vector = new ByteVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new byte[]{(byte)10, (byte)20, (byte)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((byte)10, vector.get(0));
        assertEquals((byte)20, vector.get(1));
        assertEquals((byte)30, vector.get(2));
        assertEquals(NULL_BYTE, vector.get(3));
        assertEquals(NULL_BYTE, vector.get(-1));
        byte[] bytes = vector.toArray();
        assertEquals((byte)10, bytes[0]);
        assertEquals((byte)20, bytes[1]);
        assertEquals((byte)30, bytes[2]);
        assertEquals(3, bytes.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_BYTE, vector.subVector(0, 0).get(0));
        assertEquals(NULL_BYTE, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        byte[] bytes3 = vector.subVector(0, 1).toArray();
        assertEquals(1,bytes3.length);
        assertEquals((byte)10,bytes3[0]);

        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        byte[] bytes1 = vector.subVector(1, 2).toArray();
        assertEquals(1,bytes1.length);
        assertEquals((byte)20,bytes1[0]);
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(1));
        assertEquals(NULL_BYTE, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        byte[] bytes2 = vector.subVector(1, 3).toArray();
        assertEquals(2,bytes2.length);
        assertEquals((byte)20,bytes2[0]);
        assertEquals((byte)30,bytes2[1]);
        assertEquals(NULL_BYTE,vector.subVector(1, 3).get(2));
        assertEquals(NULL_BYTE,vector.subVector(0, 1).get(-1));
    }
}

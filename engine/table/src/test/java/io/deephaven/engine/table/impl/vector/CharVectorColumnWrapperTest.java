package io.deephaven.engine.table.impl.vector;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.vector.CharVector;
import junit.framework.TestCase;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * Unit tests for {@link CharVectorColumnWrapper}.
 */
public class CharVectorColumnWrapperTest extends TestCase {

    public void testVectorColumnWrapper() {
        CharVector vector = new CharVectorColumnWrapper(
                ArrayBackedColumnSource.getMemoryColumnSource(new char[]{(char)10, (char)20, (char)30}),
                RowSetFactory.fromRange(0, 2));
        assertEquals(3, vector.size());
        assertEquals((char)10, vector.get(0));
        assertEquals((char)20, vector.get(1));
        assertEquals((char)30, vector.get(2));
        assertEquals(NULL_CHAR, vector.get(3));
        assertEquals(NULL_CHAR, vector.get(-1));
        char[] chars = vector.toArray();
        assertEquals((char)10, chars[0]);
        assertEquals((char)20, chars[1]);
        assertEquals((char)30, chars[2]);
        assertEquals(3, chars.length);
        assertEquals(0, vector.subVector(0, 0).size());
        assertEquals(0, vector.subVector(0, 0).toArray().length);
        assertEquals(NULL_CHAR, vector.subVector(0, 0).get(0));
        assertEquals(NULL_CHAR, vector.subVector(0, 0).get(-1));

        assertEquals(1, vector.subVector(0, 1).size());
        char[] chars3 = vector.subVector(0, 1).toArray();
        assertEquals(1,chars3.length);
        assertEquals((char)10,chars3[0]);

        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(1));
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(-1));

        assertEquals(1, vector.subVector(1, 2).size());
        char[] chars1 = vector.subVector(1, 2).toArray();
        assertEquals(1,chars1.length);
        assertEquals((char)20,chars1[0]);
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(1));
        assertEquals(NULL_CHAR, vector.subVector(0, 1).get(-1));

        assertEquals(2, vector.subVector(1, 3).size());
        char[] chars2 = vector.subVector(1, 3).toArray();
        assertEquals(2,chars2.length);
        assertEquals((char)20,chars2[0]);
        assertEquals((char)30,chars2[1]);
        assertEquals(NULL_CHAR,vector.subVector(1, 3).get(2));
        assertEquals(NULL_CHAR,vector.subVector(0, 1).get(-1));
    }
}

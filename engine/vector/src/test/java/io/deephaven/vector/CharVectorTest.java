//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_CHAR;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link CharVector}.
 */
public abstract class CharVectorTest {

    protected abstract CharVector makeTestVector(@NotNull final char... data);

    @Test
    public void testVector() {
        final CharVector vector = makeTestVector((char) 10, (char) 20, (char) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((char) 10, vector.get(0));
        assertEquals((char) 20, vector.get(1));
        assertEquals((char) 30, vector.get(2));
        assertEquals(NULL_CHAR, vector.get(3));
        assertEquals(NULL_CHAR, vector.get(-1));

        char[] chars = vector.toArray();
        assertEquals(vector, new CharVectorDirect(chars));
        assertEquals(vector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals((char) 10, chars[0]);
        assertEquals((char) 20, chars[1]);
        assertEquals((char) 30, chars[2]);
        assertEquals(3, chars.length);

        CharVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(NULL_CHAR, subVector.get(0));
        assertEquals(NULL_CHAR, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(1, chars.length);
        assertEquals((char) 10, chars[0]);
        assertEquals(NULL_CHAR, subVector.get(1));
        assertEquals(NULL_CHAR, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(1, chars.length);
        assertEquals((char) 30, chars[0]);
        assertEquals(NULL_CHAR, subVector.get(1));
        assertEquals(NULL_CHAR, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(2, chars.length);
        assertEquals((char) 20, chars[0]);
        assertEquals((char) 30, chars[1]);
        assertEquals(NULL_CHAR, subVector.get(2));
        assertEquals(NULL_CHAR, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(4, chars.length);
        assertEquals(NULL_CHAR, chars[0]);
        assertEquals((char) 20, chars[1]);
        assertEquals((char) 30, chars[2]);
        assertEquals(NULL_CHAR, chars[3]);
        assertEquals(NULL_CHAR, subVector.get(-1));
        assertEquals(NULL_CHAR, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        chars = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(chars));
        assertEquals(subVector.hashCode(), new CharVectorDirect(chars).hashCode());
        assertEquals(4, chars.length);
        assertEquals(NULL_CHAR, chars[0]);
        assertEquals(NULL_CHAR, chars[1]);
        assertEquals((char) 20, chars[2]);
        assertEquals(NULL_CHAR, chars[3]);
        assertEquals(NULL_CHAR, subVector.get(-1));
        assertEquals(NULL_CHAR, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final char[] data = new char[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (char) ei;
        }
        final CharVector vector = makeTestVector(data);
        assertEquals(vector, new CharVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        CharVector vector = makeTestVector((char) 10, (char) 20, (char) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final char[] result = new char[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_CHAR : vector.get(ei);
                }

                checkSubVector(vector, start, end, result);
            }
        }

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                for (int start2 = -4; start2 <= 4; start2++) {
                    for (int end2 = -1; end2 <= 7; end2++) {
                        if (start > end || start2 > end2) {
                            continue;
                        }

                        final char[] result = new char[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_CHAR : vector.get(ei);
                        }

                        final char[] result2 = new char[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_CHAR : result[ei];
                        }

                        checkDoubleSubVector(vector, start, end, start2, end2, result2);
                    }
                }
            }
        }
    }

    @Test
    public void testType() {
        // region TestType
        assertEquals(CharVector.type().clazz(), CharVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final char[] data = new char[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_CHAR : (char) ei;
        }
        final CharVector vector = makeTestVector(data);
        assertEquals(vector, new CharVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_CHAR) {
                throw new IllegalStateException("Expected null, but got boxed NULL_CHAR");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    @Test
    public void testForStreamAsIntNulls() {
        final char[] data = new char[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_CHAR : (char) ei;
        }
        final CharVector vector = makeTestVector(data);
        assertEquals(vector, new CharVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().streamAsInt().forEach(c -> {
            if (c == NULL_INT) {
                numNulls.add(1);
            } else if (c == NULL_CHAR) {
                throw new IllegalStateException("Expected NULL_INT, but got NULL_CHAR");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }
    // endregion streamAsIntTest

    private static void checkSubVector(
            final CharVector vector,
            final int start,
            final int end,
            final char[] result) {
        final CharVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final char[] array = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(array));
        assertEquals(subVector.hashCode(), new CharVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final CharVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final char[] result) {
        CharVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final char[] array = subVector.toArray();
        assertEquals(subVector, new CharVectorDirect(array));
        assertEquals(subVector.hashCode(), new CharVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final CharVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final CharVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfChar iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextChar());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

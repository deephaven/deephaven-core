//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseableIterator;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link ObjectVector}.
 */
public abstract class ObjectVectorTest {

    protected abstract ObjectVector<Object> makeTestVector(@NotNull final Object... data);

    @Test
    public void testVector() {
        final ObjectVector<Object> vector = makeTestVector(10, 20, 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals(10, vector.get(0));
        assertEquals(20, vector.get(1));
        assertEquals(30, vector.get(2));
        assertNull(vector.get(3));
        assertNull(vector.get(-1));

        Object[] Objects = vector.toArray();
        assertEquals(vector, new ObjectVectorDirect<>(Objects));
        assertEquals(vector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(10, Objects[0]);
        assertEquals(20, Objects[1]);
        assertEquals(30, Objects[2]);
        assertEquals(3, Objects.length);

        ObjectVector<Object> subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertNull(subVector.get(0));
        assertNull(subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(1, Objects.length);
        assertEquals(10, Objects[0]);
        assertNull(subVector.get(1));
        assertNull(subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(1, Objects.length);
        assertEquals(30, Objects[0]);
        assertNull(subVector.get(1));
        assertNull(subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(2, Objects.length);
        assertEquals(20, Objects[0]);
        assertEquals(30, Objects[1]);
        assertNull(subVector.get(2));
        assertNull(subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(4, Objects.length);
        assertNull(Objects[0]);
        assertEquals(20, Objects[1]);
        assertEquals(30, Objects[2]);
        assertNull(Objects[3]);
        assertNull(subVector.get(-1));
        assertNull(subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        Objects = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(Objects));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(Objects).hashCode());
        assertEquals(4, Objects.length);
        assertNull(Objects[0]);
        assertNull(Objects[1]);
        assertEquals(20, Objects[2]);
        assertNull(Objects[3]);
        assertNull(subVector.get(-1));
        assertNull(subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final Object[] data = new Object[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei;
        }
        final ObjectVector<Object> vector = makeTestVector(data);
        assertEquals(vector, new ObjectVectorDirect<>(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        ObjectVector<Object> vector = makeTestVector(10, 20, 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final Object[] result = new Object[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? null : vector.get(ei);
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

                        final Object[] result = new Object[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? null : vector.get(ei);
                        }

                        final Object[] result2 = new Object[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? null : result[ei];
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
        assertEquals(ObjectVector.type(io.deephaven.qst.type.StringType.of()).clazz(), ObjectVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final Object[] data = new Object[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? null : ei;
        }
        final ObjectVector<Object> vector = makeTestVector(data);
        assertEquals(vector, new ObjectVectorDirect<>(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == null) {
                throw new IllegalStateException("Expected null, but got boxed null");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    // endregion streamAsIntTest

    private static void checkSubVector(
            final ObjectVector<Object> vector,
            final int start,
            final int end,
            final Object[] result) {
        final ObjectVector<Object> subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final Object[] array = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(array));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final ObjectVector<Object> vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final Object[] result) {
        ObjectVector<Object> subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final Object[] array = subVector.toArray();
        assertEquals(subVector, new ObjectVectorDirect<>(array));
        assertEquals(subVector.hashCode(), new ObjectVectorDirect<>(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final ObjectVector<Object> vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final ObjectVector<Object> vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseableIterator<Object> iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.next());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

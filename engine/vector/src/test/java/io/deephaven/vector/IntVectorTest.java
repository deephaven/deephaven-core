//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_INT;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link IntVector}.
 */
public abstract class IntVectorTest {

    protected abstract IntVector makeTestVector(@NotNull final int... data);

    @Test
    public void testVector() {
        final IntVector vector = makeTestVector((int) 10, (int) 20, (int) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((int) 10, vector.get(0));
        assertEquals((int) 20, vector.get(1));
        assertEquals((int) 30, vector.get(2));
        assertEquals(NULL_INT, vector.get(3));
        assertEquals(NULL_INT, vector.get(-1));

        int[] ints = vector.toArray();
        assertEquals(vector, new IntVectorDirect(ints));
        assertEquals(vector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals((int) 10, ints[0]);
        assertEquals((int) 20, ints[1]);
        assertEquals((int) 30, ints[2]);
        assertEquals(3, ints.length);

        IntVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(NULL_INT, subVector.get(0));
        assertEquals(NULL_INT, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(1, ints.length);
        assertEquals((int) 10, ints[0]);
        assertEquals(NULL_INT, subVector.get(1));
        assertEquals(NULL_INT, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(1, ints.length);
        assertEquals((int) 30, ints[0]);
        assertEquals(NULL_INT, subVector.get(1));
        assertEquals(NULL_INT, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(2, ints.length);
        assertEquals((int) 20, ints[0]);
        assertEquals((int) 30, ints[1]);
        assertEquals(NULL_INT, subVector.get(2));
        assertEquals(NULL_INT, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(4, ints.length);
        assertEquals(NULL_INT, ints[0]);
        assertEquals((int) 20, ints[1]);
        assertEquals((int) 30, ints[2]);
        assertEquals(NULL_INT, ints[3]);
        assertEquals(NULL_INT, subVector.get(-1));
        assertEquals(NULL_INT, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        ints = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(ints));
        assertEquals(subVector.hashCode(), new IntVectorDirect(ints).hashCode());
        assertEquals(4, ints.length);
        assertEquals(NULL_INT, ints[0]);
        assertEquals(NULL_INT, ints[1]);
        assertEquals((int) 20, ints[2]);
        assertEquals(NULL_INT, ints[3]);
        assertEquals(NULL_INT, subVector.get(-1));
        assertEquals(NULL_INT, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final int[] data = new int[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (int) ei;
        }
        final IntVector vector = makeTestVector(data);
        assertEquals(vector, new IntVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        IntVector vector = makeTestVector((int) 10, (int) 20, (int) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final int[] result = new int[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_INT : vector.get(ei);
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

                        final int[] result = new int[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_INT : vector.get(ei);
                        }

                        final int[] result2 = new int[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_INT : result[ei];
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
        assertEquals(IntVector.type().clazz(), IntVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final int[] data = new int[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_INT : (int) ei;
        }
        final IntVector vector = makeTestVector(data);
        assertEquals(vector, new IntVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_INT) {
                throw new IllegalStateException("Expected null, but got boxed NULL_INT");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    // endregion streamAsIntTest

    private static void checkSubVector(
            final IntVector vector,
            final int start,
            final int end,
            final int[] result) {
        final IntVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final int[] array = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(array));
        assertEquals(subVector.hashCode(), new IntVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final IntVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final int[] result) {
        IntVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final int[] array = subVector.toArray();
        assertEquals(subVector, new IntVectorDirect(array));
        assertEquals(subVector.hashCode(), new IntVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final IntVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final IntVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfInt iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextInt());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

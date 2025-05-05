//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_LONG;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link LongVector}.
 */
public abstract class LongVectorTest {

    protected abstract LongVector makeTestVector(@NotNull final long... data);

    @Test
    public void testVector() {
        final LongVector vector = makeTestVector((long) 10, (long) 20, (long) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((long) 10, vector.get(0));
        assertEquals((long) 20, vector.get(1));
        assertEquals((long) 30, vector.get(2));
        assertEquals(NULL_LONG, vector.get(3));
        assertEquals(NULL_LONG, vector.get(-1));

        long[] longs = vector.toArray();
        assertEquals(vector, new LongVectorDirect(longs));
        assertEquals(vector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals((long) 10, longs[0]);
        assertEquals((long) 20, longs[1]);
        assertEquals((long) 30, longs[2]);
        assertEquals(3, longs.length);

        LongVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(NULL_LONG, subVector.get(0));
        assertEquals(NULL_LONG, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(1, longs.length);
        assertEquals((long) 10, longs[0]);
        assertEquals(NULL_LONG, subVector.get(1));
        assertEquals(NULL_LONG, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(1, longs.length);
        assertEquals((long) 30, longs[0]);
        assertEquals(NULL_LONG, subVector.get(1));
        assertEquals(NULL_LONG, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(2, longs.length);
        assertEquals((long) 20, longs[0]);
        assertEquals((long) 30, longs[1]);
        assertEquals(NULL_LONG, subVector.get(2));
        assertEquals(NULL_LONG, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(4, longs.length);
        assertEquals(NULL_LONG, longs[0]);
        assertEquals((long) 20, longs[1]);
        assertEquals((long) 30, longs[2]);
        assertEquals(NULL_LONG, longs[3]);
        assertEquals(NULL_LONG, subVector.get(-1));
        assertEquals(NULL_LONG, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        longs = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(longs));
        assertEquals(subVector.hashCode(), new LongVectorDirect(longs).hashCode());
        assertEquals(4, longs.length);
        assertEquals(NULL_LONG, longs[0]);
        assertEquals(NULL_LONG, longs[1]);
        assertEquals((long) 20, longs[2]);
        assertEquals(NULL_LONG, longs[3]);
        assertEquals(NULL_LONG, subVector.get(-1));
        assertEquals(NULL_LONG, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final long[] data = new long[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (long) ei;
        }
        final LongVector vector = makeTestVector(data);
        assertEquals(vector, new LongVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        LongVector vector = makeTestVector((long) 10, (long) 20, (long) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final long[] result = new long[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_LONG : vector.get(ei);
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

                        final long[] result = new long[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_LONG : vector.get(ei);
                        }

                        final long[] result2 = new long[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_LONG : result[ei];
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
        assertEquals(LongVector.type().clazz(), LongVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final long[] data = new long[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_LONG : (long) ei;
        }
        final LongVector vector = makeTestVector(data);
        assertEquals(vector, new LongVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_LONG) {
                throw new IllegalStateException("Expected null, but got boxed NULL_LONG");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    // endregion streamAsIntTest

    private static void checkSubVector(
            final LongVector vector,
            final int start,
            final int end,
            final long[] result) {
        final LongVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final long[] array = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(array));
        assertEquals(subVector.hashCode(), new LongVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final LongVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final long[] result) {
        LongVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final long[] array = subVector.toArray();
        assertEquals(subVector, new LongVectorDirect(array));
        assertEquals(subVector.hashCode(), new LongVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final LongVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final LongVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfLong iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextLong());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

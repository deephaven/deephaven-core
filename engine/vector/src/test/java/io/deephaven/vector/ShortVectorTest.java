//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_SHORT;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link ShortVector}.
 */
public abstract class ShortVectorTest {

    protected abstract ShortVector makeTestVector(@NotNull final short... data);

    @Test
    public void testVector() {
        final ShortVector vector = makeTestVector((short) 10, (short) 20, (short) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((short) 10, vector.get(0));
        assertEquals((short) 20, vector.get(1));
        assertEquals((short) 30, vector.get(2));
        assertEquals(NULL_SHORT, vector.get(3));
        assertEquals(NULL_SHORT, vector.get(-1));

        short[] shorts = vector.toArray();
        assertEquals(vector, new ShortVectorDirect(shorts));
        assertEquals(vector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals((short) 10, shorts[0]);
        assertEquals((short) 20, shorts[1]);
        assertEquals((short) 30, shorts[2]);
        assertEquals(3, shorts.length);

        ShortVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(NULL_SHORT, subVector.get(0));
        assertEquals(NULL_SHORT, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(1, shorts.length);
        assertEquals((short) 10, shorts[0]);
        assertEquals(NULL_SHORT, subVector.get(1));
        assertEquals(NULL_SHORT, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(1, shorts.length);
        assertEquals((short) 30, shorts[0]);
        assertEquals(NULL_SHORT, subVector.get(1));
        assertEquals(NULL_SHORT, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(2, shorts.length);
        assertEquals((short) 20, shorts[0]);
        assertEquals((short) 30, shorts[1]);
        assertEquals(NULL_SHORT, subVector.get(2));
        assertEquals(NULL_SHORT, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(4, shorts.length);
        assertEquals(NULL_SHORT, shorts[0]);
        assertEquals((short) 20, shorts[1]);
        assertEquals((short) 30, shorts[2]);
        assertEquals(NULL_SHORT, shorts[3]);
        assertEquals(NULL_SHORT, subVector.get(-1));
        assertEquals(NULL_SHORT, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        shorts = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(shorts));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(shorts).hashCode());
        assertEquals(4, shorts.length);
        assertEquals(NULL_SHORT, shorts[0]);
        assertEquals(NULL_SHORT, shorts[1]);
        assertEquals((short) 20, shorts[2]);
        assertEquals(NULL_SHORT, shorts[3]);
        assertEquals(NULL_SHORT, subVector.get(-1));
        assertEquals(NULL_SHORT, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final short[] data = new short[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (short) ei;
        }
        final ShortVector vector = makeTestVector(data);
        assertEquals(vector, new ShortVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        ShortVector vector = makeTestVector((short) 10, (short) 20, (short) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final short[] result = new short[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_SHORT : vector.get(ei);
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

                        final short[] result = new short[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_SHORT : vector.get(ei);
                        }

                        final short[] result2 = new short[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_SHORT : result[ei];
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
        assertEquals(ShortVector.type().clazz(), ShortVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final short[] data = new short[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_SHORT : (short) ei;
        }
        final ShortVector vector = makeTestVector(data);
        assertEquals(vector, new ShortVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_SHORT) {
                throw new IllegalStateException("Expected null, but got boxed NULL_SHORT");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    @Test
    public void testForStreamAsIntNulls() {
        final short[] data = new short[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_SHORT : (short) ei;
        }
        final ShortVector vector = makeTestVector(data);
        assertEquals(vector, new ShortVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().streamAsInt().forEach(c -> {
            if (c == NULL_INT) {
                numNulls.add(1);
            } else if (c == NULL_SHORT) {
                throw new IllegalStateException("Expected NULL_INT, but got NULL_SHORT");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }
    // endregion streamAsIntTest

    private static void checkSubVector(
            final ShortVector vector,
            final int start,
            final int end,
            final short[] result) {
        final ShortVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final short[] array = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(array));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final ShortVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final short[] result) {
        ShortVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final short[] array = subVector.toArray();
        assertEquals(subVector, new ShortVectorDirect(array));
        assertEquals(subVector.hashCode(), new ShortVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final ShortVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final ShortVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfShort iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextShort());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link FloatVector}.
 */
public abstract class FloatVectorTest {

    protected abstract FloatVector makeTestVector(@NotNull final float... data);

    @Test
    public void testVector() {
        final FloatVector vector = makeTestVector((float) 10, (float) 20, (float) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((float) 10, vector.get(0));
        assertEquals((float) 20, vector.get(1));
        assertEquals((float) 30, vector.get(2));
        assertEquals(NULL_FLOAT, vector.get(3));
        assertEquals(NULL_FLOAT, vector.get(-1));

        float[] floats = vector.toArray();
        assertEquals(vector, new FloatVectorDirect(floats));
        assertEquals(vector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals((float) 10, floats[0]);
        assertEquals((float) 20, floats[1]);
        assertEquals((float) 30, floats[2]);
        assertEquals(3, floats.length);

        FloatVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(NULL_FLOAT, subVector.get(0));
        assertEquals(NULL_FLOAT, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(1, floats.length);
        assertEquals((float) 10, floats[0]);
        assertEquals(NULL_FLOAT, subVector.get(1));
        assertEquals(NULL_FLOAT, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(1, floats.length);
        assertEquals((float) 30, floats[0]);
        assertEquals(NULL_FLOAT, subVector.get(1));
        assertEquals(NULL_FLOAT, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(2, floats.length);
        assertEquals((float) 20, floats[0]);
        assertEquals((float) 30, floats[1]);
        assertEquals(NULL_FLOAT, subVector.get(2));
        assertEquals(NULL_FLOAT, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(4, floats.length);
        assertEquals(NULL_FLOAT, floats[0]);
        assertEquals((float) 20, floats[1]);
        assertEquals((float) 30, floats[2]);
        assertEquals(NULL_FLOAT, floats[3]);
        assertEquals(NULL_FLOAT, subVector.get(-1));
        assertEquals(NULL_FLOAT, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        floats = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(floats));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(floats).hashCode());
        assertEquals(4, floats.length);
        assertEquals(NULL_FLOAT, floats[0]);
        assertEquals(NULL_FLOAT, floats[1]);
        assertEquals((float) 20, floats[2]);
        assertEquals(NULL_FLOAT, floats[3]);
        assertEquals(NULL_FLOAT, subVector.get(-1));
        assertEquals(NULL_FLOAT, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final float[] data = new float[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (float) ei;
        }
        final FloatVector vector = makeTestVector(data);
        assertEquals(vector, new FloatVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        FloatVector vector = makeTestVector((float) 10, (float) 20, (float) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final float[] result = new float[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_FLOAT : vector.get(ei);
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

                        final float[] result = new float[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_FLOAT : vector.get(ei);
                        }

                        final float[] result2 = new float[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_FLOAT : result[ei];
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
        assertEquals(FloatVector.type().clazz(), FloatVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final float[] data = new float[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_FLOAT : (float) ei;
        }
        final FloatVector vector = makeTestVector(data);
        assertEquals(vector, new FloatVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_FLOAT) {
                throw new IllegalStateException("Expected null, but got boxed NULL_FLOAT");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsDoubleTest
    @Test
    public void testForStreamAsIntNulls() {
        final float[] data = new float[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_FLOAT : (float) ei;
        }
        final FloatVector vector = makeTestVector(data);
        assertEquals(vector, new FloatVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().streamAsDouble().forEach(c -> {
            if (c == NULL_DOUBLE) {
                numNulls.add(1);
            } else if (c == NULL_FLOAT) {
                throw new IllegalStateException("Expected NULL_DOUBLE, but got NULL_FLOAT");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }
    // endregion streamAsIntTest

    private static void checkSubVector(
            final FloatVector vector,
            final int start,
            final int end,
            final float[] result) {
        final FloatVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final float[] array = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(array));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final FloatVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final float[] result) {
        FloatVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final float[] array = subVector.toArray();
        assertEquals(subVector, new FloatVectorDirect(array));
        assertEquals(subVector.hashCode(), new FloatVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final FloatVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final FloatVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfFloat iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextFloat());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

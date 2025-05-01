//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link DoubleVector}.
 */
public abstract class DoubleVectorTest {

    protected abstract DoubleVector makeTestVector(@NotNull final double... data);

    @Test
    public void testVector() {
        final DoubleVector vector = makeTestVector((double) 10, (double) 20, (double) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((double) 10, vector.get(0));
        assertEquals((double) 20, vector.get(1));
        assertEquals((double) 30, vector.get(2));
        assertEquals(NULL_DOUBLE, vector.get(3));
        assertEquals(NULL_DOUBLE, vector.get(-1));

        double[] doubles = vector.toArray();
        assertEquals(vector, new DoubleVectorDirect(doubles));
        assertEquals(vector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals((double) 10, doubles[0]);
        assertEquals((double) 20, doubles[1]);
        assertEquals((double) 30, doubles[2]);
        assertEquals(3, doubles.length);

        DoubleVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(NULL_DOUBLE, subVector.get(0));
        assertEquals(NULL_DOUBLE, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(1, doubles.length);
        assertEquals((double) 10, doubles[0]);
        assertEquals(NULL_DOUBLE, subVector.get(1));
        assertEquals(NULL_DOUBLE, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(1, doubles.length);
        assertEquals((double) 30, doubles[0]);
        assertEquals(NULL_DOUBLE, subVector.get(1));
        assertEquals(NULL_DOUBLE, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(2, doubles.length);
        assertEquals((double) 20, doubles[0]);
        assertEquals((double) 30, doubles[1]);
        assertEquals(NULL_DOUBLE, subVector.get(2));
        assertEquals(NULL_DOUBLE, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(4, doubles.length);
        assertEquals(NULL_DOUBLE, doubles[0]);
        assertEquals((double) 20, doubles[1]);
        assertEquals((double) 30, doubles[2]);
        assertEquals(NULL_DOUBLE, doubles[3]);
        assertEquals(NULL_DOUBLE, subVector.get(-1));
        assertEquals(NULL_DOUBLE, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        doubles = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(doubles));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(doubles).hashCode());
        assertEquals(4, doubles.length);
        assertEquals(NULL_DOUBLE, doubles[0]);
        assertEquals(NULL_DOUBLE, doubles[1]);
        assertEquals((double) 20, doubles[2]);
        assertEquals(NULL_DOUBLE, doubles[3]);
        assertEquals(NULL_DOUBLE, subVector.get(-1));
        assertEquals(NULL_DOUBLE, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final double[] data = new double[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (double) ei;
        }
        final DoubleVector vector = makeTestVector(data);
        assertEquals(vector, new DoubleVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        DoubleVector vector = makeTestVector((double) 10, (double) 20, (double) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final double[] result = new double[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_DOUBLE : vector.get(ei);
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

                        final double[] result = new double[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_DOUBLE : vector.get(ei);
                        }

                        final double[] result2 = new double[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_DOUBLE : result[ei];
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
        assertEquals(DoubleVector.type().clazz(), DoubleVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final double[] data = new double[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_DOUBLE : (double) ei;
        }
        final DoubleVector vector = makeTestVector(data);
        assertEquals(vector, new DoubleVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_DOUBLE) {
                throw new IllegalStateException("Expected null, but got boxed NULL_DOUBLE");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    // endregion streamAsIntTest

    private static void checkSubVector(
            final DoubleVector vector,
            final int start,
            final int end,
            final double[] result) {
        final DoubleVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final double[] array = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(array));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final DoubleVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final double[] result) {
        DoubleVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final double[] array = subVector.toArray();
        assertEquals(subVector, new DoubleVectorDirect(array));
        assertEquals(subVector.hashCode(), new DoubleVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final DoubleVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final DoubleVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfDouble iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextDouble());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

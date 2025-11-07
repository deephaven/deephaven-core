//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorTest and run "./gradlew replicateVectorTests" to regenerate
//
// @formatter:off
package io.deephaven.vector;

// region IteratorTypeImport
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
// endregion IteratorTypeImport
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

// region NullConstantImport
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
// endregion NullConstantImport
import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;

/**
 * Unit tests for various permutations of {@link ByteVector}.
 */
public abstract class ByteVectorTest {

    protected abstract ByteVector makeTestVector(@NotNull final byte... data);

    @Test
    public void testVector() {
        final ByteVector vector = makeTestVector((byte) 10, (byte) 20, (byte) 30);
        validateIterator(vector);
        assertEquals(3, vector.size());
        assertEquals((byte) 10, vector.get(0));
        assertEquals((byte) 20, vector.get(1));
        assertEquals((byte) 30, vector.get(2));
        assertEquals(NULL_BYTE, vector.get(3));
        assertEquals(NULL_BYTE, vector.get(-1));

        byte[] bytes = vector.toArray();
        assertEquals(vector, new ByteVectorDirect(bytes));
        assertEquals(vector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals((byte) 10, bytes[0]);
        assertEquals((byte) 20, bytes[1]);
        assertEquals((byte) 30, bytes[2]);
        assertEquals(3, bytes.length);

        ByteVector subVector = vector.subVector(0, 0);
        validateIterator(subVector);
        assertEquals(0, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(NULL_BYTE, subVector.get(0));
        assertEquals(NULL_BYTE, subVector.get(-1));

        subVector = vector.subVector(0, 1);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(1, bytes.length);
        assertEquals((byte) 10, bytes[0]);
        assertEquals(NULL_BYTE, subVector.get(1));
        assertEquals(NULL_BYTE, subVector.get(-1));

        subVector = vector.subVector(2, 3);
        validateIterator(subVector);
        assertEquals(1, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(1, bytes.length);
        assertEquals((byte) 30, bytes[0]);
        assertEquals(NULL_BYTE, subVector.get(1));
        assertEquals(NULL_BYTE, subVector.get(-1));

        subVector = vector.subVector(1, 3);
        validateIterator(subVector);
        assertEquals(2, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(2, bytes.length);
        assertEquals((byte) 20, bytes[0]);
        assertEquals((byte) 30, bytes[1]);
        assertEquals(NULL_BYTE, subVector.get(2));
        assertEquals(NULL_BYTE, subVector.get(-1));

        subVector = vector.subVectorByPositions(new long[] {-1, 1, 2, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(4, bytes.length);
        assertEquals(NULL_BYTE, bytes[0]);
        assertEquals((byte) 20, bytes[1]);
        assertEquals((byte) 30, bytes[2]);
        assertEquals(NULL_BYTE, bytes[3]);
        assertEquals(NULL_BYTE, subVector.get(-1));
        assertEquals(NULL_BYTE, subVector.get(4));

        subVector = subVector.subVectorByPositions(new long[] {-1, 0, 1, 4});
        validateIterator(subVector);
        assertEquals(4, subVector.size());
        bytes = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(bytes));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(bytes).hashCode());
        assertEquals(4, bytes.length);
        assertEquals(NULL_BYTE, bytes[0]);
        assertEquals(NULL_BYTE, bytes[1]);
        assertEquals((byte) 20, bytes[2]);
        assertEquals(NULL_BYTE, bytes[3]);
        assertEquals(NULL_BYTE, subVector.get(-1));
        assertEquals(NULL_BYTE, subVector.get(4));

        subVector = subVector.subVector(0, 2);
        validateIterator(subVector);

        subVector = subVector.subVectorByPositions(new long[] {0, 1, 2});
        validateIterator(subVector);
    }

    @Test
    public void testLarge() {
        final byte[] data = new byte[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = (byte) ei;
        }
        final ByteVector vector = makeTestVector(data);
        assertEquals(vector, new ByteVectorDirect(vector.toArray()));
        validateIterator(vector, -data.length, data.length * 2);
        validateIterator(vector, -5, data.length / 2);
        validateIterator(vector, -5, data.length);
        validateIterator(vector, data.length / 2, data.length);
        validateIterator(vector, data.length / 2, data.length + 5);
        validateIterator(vector, data.length, data.length + 5);
    }

    @Test
    public void testSubVector() {
        ByteVector vector = makeTestVector((byte) 10, (byte) 20, (byte) 30);
        validateIterator(vector);

        for (int start = -4; start <= 4; start++) {
            for (int end = -1; end <= 7; end++) {
                if (start > end) {
                    continue;
                }

                final byte[] result = new byte[end - start];

                for (int ei = start; ei < end; ei++) {
                    result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_BYTE : vector.get(ei);
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

                        final byte[] result = new byte[end - start];

                        for (int ei = start; ei < end; ei++) {
                            result[ei - start] = (ei < 0 || ei >= vector.size()) ? NULL_BYTE : vector.get(ei);
                        }

                        final byte[] result2 = new byte[end2 - start2];

                        for (int ei = start2; ei < end2; ei++) {
                            result2[ei - start2] = (ei < 0 || ei >= result.length) ? NULL_BYTE : result[ei];
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
        assertEquals(ByteVector.type().clazz(), ByteVector.class);
        // endregion TestType
    }

    @Test
    public void testForBoxedNulls() {
        final byte[] data = new byte[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_BYTE : (byte) ei;
        }
        final ByteVector vector = makeTestVector(data);
        assertEquals(vector, new ByteVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().stream().forEach(c -> {
            if (c == null) {
                numNulls.add(1);
            } else if (c == NULL_BYTE) {
                throw new IllegalStateException("Expected null, but got boxed NULL_BYTE");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }

    // region streamAsIntTest
    @Test
    public void testForStreamAsIntNulls() {
        final byte[] data = new byte[10_000];
        for (int ei = 0; ei < data.length; ++ei) {
            data[ei] = ei % 4 == 0 ? NULL_BYTE : (byte) ei;
        }
        final ByteVector vector = makeTestVector(data);
        assertEquals(vector, new ByteVectorDirect(vector.toArray()));
        final MutableInt numNulls = new MutableInt(0);
        vector.iterator().streamAsInt().forEach(c -> {
            if (c == NULL_INT) {
                numNulls.add(1);
            } else if (c == NULL_BYTE) {
                throw new IllegalStateException("Expected NULL_INT, but got NULL_BYTE");
            }
        });
        assertEquals("numNulls.get() == (data.length + 3) / 4", (data.length + 3) / 4, numNulls.get());
    }
    // endregion streamAsIntTest

    private static void checkSubVector(
            final ByteVector vector,
            final int start,
            final int end,
            final byte[] result) {
        final ByteVector subVector = vector.subVector(start, end);
        validateIterator(subVector);
        final byte[] array = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(array));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void checkDoubleSubVector(
            final ByteVector vector,
            final int start,
            final int end,
            final int start2,
            final int end2,
            final byte[] result) {
        ByteVector subVector = vector.subVector(start, end);
        subVector = subVector.subVector(start2, end2);
        validateIterator(subVector);
        final byte[] array = subVector.toArray();
        assertEquals(subVector, new ByteVectorDirect(array));
        assertEquals(subVector.hashCode(), new ByteVectorDirect(array).hashCode());
        assertEquals(result.length, subVector.size());
        assertEquals(result.length, array.length);

        for (int ei = 0; ei < result.length; ei++) {
            assertEquals(result[ei], subVector.get(ei));
            assertEquals(result[ei], array[ei]);
        }
    }

    private static void validateIterator(@NotNull final ByteVector vector) {
        for (long si = -2; si < vector.size() + 2; ++si) {
            for (long ei = si; ei < vector.size() + 2; ++ei) {
                validateIterator(vector, si, ei);
            }
        }
    }

    private static void validateIterator(
            @NotNull final ByteVector vector,
            final long startIndexInclusive,
            final long endIndexExclusive) {
        try (final CloseablePrimitiveIteratorOfByte iterator =
                vector.iterator(startIndexInclusive, endIndexExclusive)) {
            for (long vi = startIndexInclusive; vi < endIndexExclusive; ++vi) {
                assertEquals(vector.get(vi), iterator.nextByte());
            }
            assertFalse(iterator.hasNext());
        }
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIterator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.InstantSsmSourceWrapper;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.LongSsmBackedSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for the Instant view of a {@link LongSegmentedSortedMultiset}.
 *
 * <p>
 * The Instant view is not a replicated type: {@code LongSegmentedSortedMultiset} gains it from the hand-written block
 * that {@code ReplicateSegmentedSortedMultiset} inserts into its {@code Extensions} region, and
 * {@link InstantSsmSourceWrapper} is a one-off wrapper with no {@code Char} counterpart. Its coverage is therefore
 * hand-written and hand-maintained here, rather than generated from the {@code Char} test.
 */
@Category(ParallelTest.class)
public class TestInstantSegmentedSortedMultiset extends RefreshingTableTestCase {

    private static final long BASE_NANOS = 1_600_000_000_000_000_000L;

    /**
     * The value counts every check sweeps: the empty, singleton, partial-leaf, exactly-full-leaf, two-full-leaf, and
     * many-leaf representations, at the node size below.
     */
    private static final int[] VALUE_COUNTS = new int[] {0, 1, 3, 4, 8, 20};
    private static final int NODE_SIZE = 4;

    /**
     * The Instant extensions are a second face on the leaves the long accessors already read, so every Instant result
     * must be exactly the corresponding long result passed through {@link DateTimeUtils#epochNanosToInstant}. Pin the
     * two views together rather than restate the expected contents: it is the drift between them that a hand-written
     * extension to replicated code can introduce.
     */
    public void testInstantExtensions() {
        for (final int valueCount : VALUE_COUNTS) {
            checkInstantExtensions(NODE_SIZE, valueCount);
        }
    }

    public void testInstantSubArrays() {
        for (final int valueCount : VALUE_COUNTS) {
            checkInstantSubArrays(NODE_SIZE, valueCount);
        }
    }

    public void testInstantSsmSourceWrapper() {
        for (final int valueCount : VALUE_COUNTS) {
            checkInstantSsmSourceWrapper(NODE_SIZE, valueCount);
        }
    }

    /**
     * {@link InstantSsmSourceWrapper#getPrev} has two shapes to cover: an SSM that has accumulated no deltas returns
     * itself as its own previous value and must be wrapped in place, while one that has accumulated deltas returns a
     * materialized LongVector that must be converted element by element.
     */
    public void testInstantSsmSourceWrapperColumnSource() {
        final LongSsmBackedSource source = new LongSsmBackedSource();
        source.ensureCapacity(2);
        final InstantSsmSourceWrapper wrapper = new InstantSsmSourceWrapper(source);

        assertEquals(ObjectVector.class, wrapper.getType());
        assertEquals(Instant.class, wrapper.getComponentType());
        assertFalse(wrapper.isImmutable());

        final long[] initial = instantNanos(6);
        final LongSegmentedSortedMultiset ssm = source.getOrCreate(0);
        insertNanos(ssm, initial);

        assertArrayEquals(asInstants(initial), wrapper.get(0).toArray());

        // no deltas accumulated: the previous values are the SSM itself, which getPrev wraps rather than copies
        assertTrue(wrapper.getPrev(0) instanceof InstantSsmSourceWrapper.ValueWrapper);
        assertArrayEquals(asInstants(initial), wrapper.getPrev(0).toArray());

        // deltas accumulated: the previous values are a materialized LongVector, converted one element at a time
        ssm.setTrackDeltas(true);
        insertNanos(ssm, new long[] {BASE_NANOS + 6 * 1_000_000L});
        assertFalse(wrapper.getPrev(0) instanceof InstantSsmSourceWrapper.ValueWrapper);
        assertArrayEquals(asInstants(initial), wrapper.getPrev(0).toArray());
        assertArrayEquals(asInstants(instantNanos(7)), wrapper.get(0).toArray());

        // a row that never had an SSM has no previous value at all
        assertNull(wrapper.getPrev(1));
    }

    private void checkInstantExtensions(final int nodeSize, final int valueCount) {
        final long[] nanos = instantNanos(valueCount);
        final Instant[] expected = asInstants(nanos);
        final LongSegmentedSortedMultiset ssm = makeSsm(nodeSize, nanos);
        final String message = "valueCount=" + valueCount;

        for (int ii = 0; ii < valueCount; ++ii) {
            assertEquals(message, expected[ii], ssm.getAsInstant(ii));
        }

        assertArrayEquals(message, expected, ssm.toInstantArray());
        assertEquals(message, new ObjectVectorDirect<>(expected), ssm.getDirectAsInstants());
        assertEquals(message, Arrays.toString(expected), ssm.toInstantString());

        final ObjectChunk<Instant, Values> asChunk = ssm.toInstantChunk().asObjectChunk();
        assertEquals(message, valueCount, asChunk.size());
        for (int ii = 0; ii < valueCount; ++ii) {
            assertEquals(message, expected[ii], asChunk.get(ii));
        }

        // fillInstantChunk writes into a caller-supplied chunk; only the occupied prefix may be touched
        try (final WritableObjectChunk<Instant, Values> filled =
                WritableObjectChunk.makeWritableChunk(valueCount + 1)) {
            filled.fillWithNullValue(0, filled.size());
            ssm.fillInstantChunk(filled);
            for (int ii = 0; ii < valueCount; ++ii) {
                assertEquals(message, expected[ii], filled.get(ii));
            }
            assertNull(message, filled.get(valueCount));
        }
    }

    private void checkInstantSubArrays(final int nodeSize, final int valueCount) {
        final long[] nanos = instantNanos(valueCount);
        final Instant[] expected = asInstants(nanos);
        final LongSegmentedSortedMultiset ssm = makeSsm(nodeSize, nanos);

        // the reference implementation of the slice contract, including the nulls owed at offsets outside [0, size())
        final ObjectVector<Instant> reference = new ObjectVectorDirect<>(expected);

        // the Instant sub-array is exclusive at its end, exactly like the long subVector it mirrors, and must accept
        // offsets outside [0, size()) -- which read as null
        for (int from = -3; from <= valueCount + 3; ++from) {
            for (int to = from; to <= valueCount + 3; ++to) {
                final String message = "valueCount=" + valueCount + ", from=" + from + ", to=" + to;
                final LongVector asLongs = ssm.subVector(from, to);
                final ObjectVector<Instant> asInstants = ssm.subArrayAsInstants(from, to);
                final ObjectVector<Instant> expectedSlice = reference.subVector(from, to);
                assertEquals(message, (long) (to - from), asInstants.size());
                assertEquals(message, asLongs.size(), asInstants.size());
                for (int ii = 0; ii < to - from; ++ii) {
                    assertEquals(message, expectedSlice.get(ii), asInstants.get(ii));
                    // the two faces of the same leaves must agree
                    assertEquals(message, DateTimeUtils.epochNanosToInstant(asLongs.get(ii)), asInstants.get(ii));
                }
            }
        }

        // positions are read individually, in the order given, may repeat, and may fall outside [0, size())
        final long[] positions = new long[] {valueCount - 1, -1, 0, valueCount, valueCount / 2, 0};
        assertArrayEquals("valueCount=" + valueCount,
                reference.subVectorByPositions(positions).toArray(),
                ssm.subArrayByPositionsAsInstants(positions).toArray());
    }

    private void checkInstantSsmSourceWrapper(final int nodeSize, final int valueCount) {
        final long[] nanos = instantNanos(valueCount);
        final Instant[] expected = asInstants(nanos);
        final LongSegmentedSortedMultiset ssm = makeSsm(nodeSize, nanos);
        final InstantSsmSourceWrapper.ValueWrapper wrapper = new InstantSsmSourceWrapper.ValueWrapper(ssm);
        final String message = "valueCount=" + valueCount;

        assertEquals(message, Instant.class, wrapper.getComponentType());
        assertEquals(message, valueCount, wrapper.size());
        assertEquals(message, valueCount, wrapper.intSize());
        assertEquals(message, valueCount, wrapper.intSize("checkInstantSsmSourceWrapper"));
        assertEquals(message, valueCount == 0, wrapper.isEmpty());
        assertEquals(message, ssm.toInstantString(), wrapper.toString());

        for (int ii = 0; ii < valueCount; ++ii) {
            assertEquals(message, expected[ii], wrapper.get(ii));
        }
        // offsets outside [0, size()) read as null rather than throwing
        assertNull(message, wrapper.get(-1));
        assertNull(message, wrapper.get(valueCount));
        assertNull(message, wrapper.get(Long.MAX_VALUE));
        assertArrayEquals(message, expected, wrapper.toArray());
        assertArrayEquals(message, expected, wrapper.copyToArray());

        // the wrapper overrides iterator(from, to) to keep traversal linear instead of inheriting the positional
        // default; it must still visit exactly what get() reports, over the whole vector and over every sub-range
        try (final ValueIterator<Instant> it = wrapper.iterator()) {
            assertEquals(message, valueCount, it.remaining());
            for (int ii = 0; ii < valueCount; ++ii) {
                assertTrue(message, it.hasNext());
                assertEquals(message, expected[ii], it.next());
            }
            assertFalse(message, it.hasNext());
        }
        // the reference implementation of the slice contract, including the nulls owed at offsets outside [0, size())
        final ObjectVector<Instant> reference = new ObjectVectorDirect<>(expected);
        for (int from = -3; from <= valueCount + 3; ++from) {
            for (int to = from; to <= valueCount + 3; ++to) {
                final String rangeMessage = message + ", from=" + from + ", to=" + to;
                try (final ValueIterator<Instant> it = wrapper.iterator(from, to)) {
                    assertEquals(rangeMessage, to - from, it.remaining());
                    for (int ii = from; ii < to; ++ii) {
                        assertTrue(rangeMessage, it.hasNext());
                        assertEquals(rangeMessage, reference.get(ii), it.next());
                        assertEquals(rangeMessage, to - ii - 1, it.remaining());
                    }
                    assertFalse(rangeMessage, it.hasNext());

                    // an exhausted iterator must not hand back whatever value happens to be stored next
                    try {
                        it.next();
                        fail(rangeMessage + ": expected a NoSuchElementException from an exhausted iterator");
                    } catch (NoSuchElementException expectedException) {
                        // expected
                    }
                }

                // sub-vector bounds are exclusive at the end, matching the iterator's
                final ObjectVector<Instant> sub = wrapper.subVector(from, to);
                final ObjectVector<Instant> expectedSlice = reference.subVector(from, to);
                assertEquals(rangeMessage, (long) (to - from), sub.size());
                for (int ii = 0; ii < to - from; ++ii) {
                    assertEquals(rangeMessage, expectedSlice.get(ii), sub.get(ii));
                }
            }
        }

        // positions are read individually, in the order given, may repeat, and may fall outside [0, size())
        final long[] positions = new long[] {valueCount - 1, -1, 0, valueCount, valueCount / 2, 0};
        assertArrayEquals(message, reference.subVectorByPositions(positions).toArray(),
                wrapper.subVectorByPositions(positions).toArray());

        // the wrapper must be interchangeable with any ObjectVector holding the same Instants -- notably its own
        // getDirect() result -- however those Instants happen to be laid out across leaves, and hash alike
        assertEqualBothWays(wrapper, wrapper.getDirect());
        assertEqualBothWays(wrapper, new ObjectVectorDirect<>(expected));
        assertEqualBothWays(wrapper, new InstantSsmSourceWrapper.ValueWrapper(makeSsm(nodeSize * 16, nanos)));
        assertEquals(message, ObjectVector.hashCode(wrapper), wrapper.hashCode());

        // a vector of a different length is not equal, under either layout
        final long[] longerNanos = instantNanos(valueCount + 1);
        assertNotEqualBothWays(wrapper, new ObjectVectorDirect<>(asInstants(longerNanos)));
        assertNotEqualBothWays(wrapper, new InstantSsmSourceWrapper.ValueWrapper(makeSsm(nodeSize, longerNanos)));

        // ... and neither is one that differs in a single position; check the first, middle, and last
        final Instant different = DateTimeUtils.epochNanosToInstant(BASE_NANOS - 1);
        for (int ii = 0; valueCount > 0 && ii < 3; ++ii) {
            final int position = new int[] {0, valueCount / 2, valueCount - 1}[ii];
            final Instant[] modified = expected.clone();
            modified[position] = different;
            assertNotEqualBothWays(wrapper, new ObjectVectorDirect<>(modified));
        }
    }

    /**
     * Assert that two Vectors agree that they are equal no matter which is the receiver, and that they hash alike as
     * {@link Object#hashCode()} then requires.
     */
    private void assertEqualBothWays(Object lhs, Object rhs) {
        assertTrue(lhs + " should equal " + rhs, lhs.equals(rhs));
        assertTrue(rhs + " should equal " + lhs, rhs.equals(lhs));
        assertEquals("equal values must hash alike", lhs.hashCode(), rhs.hashCode());
    }

    /**
     * Assert that two Vectors agree that they are unequal no matter which is the receiver. Their hash codes are
     * unconstrained -- unequal values are permitted to collide.
     */
    private void assertNotEqualBothWays(Object lhs, Object rhs) {
        assertFalse(lhs + " should not equal " + rhs, lhs.equals(rhs));
        assertFalse(rhs + " should not equal " + lhs, rhs.equals(lhs));
    }

    private static LongSegmentedSortedMultiset makeSsm(final int nodeSize, final long[] nanos) {
        final LongSegmentedSortedMultiset ssm = new LongSegmentedSortedMultiset(nodeSize);
        if (nanos.length > 0) {
            insertNanos(ssm, nanos);
        }
        return ssm;
    }

    private static long[] instantNanos(final int valueCount) {
        final long[] nanos = new long[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            nanos[ii] = BASE_NANOS + ii * 1_000_000L;
        }
        return nanos;
    }

    private static Instant[] asInstants(final long[] nanos) {
        final Instant[] instants = new Instant[nanos.length];
        for (int ii = 0; ii < nanos.length; ++ii) {
            instants[ii] = DateTimeUtils.epochNanosToInstant(nanos[ii]);
        }
        return instants;
    }

    private static void insertNanos(final LongSegmentedSortedMultiset ssm, final long[] nanos) {
        try (final WritableLongChunk<Values> valuesChunk = WritableLongChunk.makeWritableChunk(nanos.length);
                final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(nanos.length)) {
            for (int ii = 0; ii < nanos.length; ++ii) {
                valuesChunk.set(ii, nanos[ii]);
                countsChunk.set(ii, 1);
            }
            ssm.insert(valuesChunk, countsChunk);
        }
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link BaseChunkWriter.ValidityBuffer}, which packs the Arrow validity bitmap and counts nulls in a single
 * traversal.
 * <p>
 * The buffer only materializes its bytes once a null appears, back-filling the all-valid words it skipped, so its
 * behavior turns over at 64-element word boundaries. Everything here is therefore driven across those boundaries
 * explicitly; the expected bitmaps come from a deliberately independent byte-oriented reference
 * ({@link #expectedBitmap}) rather than from the word-oriented little-endian packing the implementation uses.
 */
public class ValidityBufferTest {

    /**
     * Arrow validity bitmap for {@code isNull}, computed a byte at a time: bit {@code ii} is set when element
     * {@code ii} is valid, and bits past the last element stay clear.
     */
    private static byte[] expectedBitmap(final boolean[] isNull) {
        final byte[] expected = new byte[((isNull.length + 63) / 64) * 8];
        for (int ii = 0; ii < isNull.length; ++ii) {
            if (!isNull[ii]) {
                expected[ii / 8] |= (byte) (1 << (ii & 7));
            }
        }
        return expected;
    }

    private static int countNulls(final boolean[] isNull) {
        int nulls = 0;
        for (final boolean elementIsNull : isNull) {
            if (elementIsNull) {
                ++nulls;
            }
        }
        return nulls;
    }

    /**
     * Feed {@code isNull} through a buffer and assert both outputs. The null count is read first, matching the order
     * {@code BaseChunkInputStream} uses: it needs the count for the field node before the bytes are drained.
     */
    private static void assertPacksTo(final String description, final boolean[] isNull) {
        final BaseChunkWriter.ValidityBuffer validity = new BaseChunkWriter.ValidityBuffer(isNull.length);
        for (final boolean elementIsNull : isNull) {
            validity.setNextIsNull(elementIsNull);
        }
        assertThat(validity.nullCount()).as("%s: nullCount", description).isEqualTo(countNulls(isNull));
        assertThat(validity.bytes()).as("%s: bitmap", description).containsExactly(expectedBitmap(isNull));
    }

    /** Sizes that bracket the word boundaries: empty, sub-word, exact multiples, and one either side. */
    private static final int[] SIZES = {0, 1, 7, 8, 62, 63, 64, 65, 66, 127, 128, 129, 133, 192, 193};

    /** Positions where a run of nulls or valid elements starting there straddles a word boundary. */
    private static final int[] BOUNDARY_POSITIONS = {0, 1, 61, 62, 63, 64, 65, 66, 126, 127, 128, 129, 130};

    @Test
    public void allElementsNull() {
        for (final int size : SIZES) {
            final boolean[] isNull = new boolean[size];
            Arrays.fill(isNull, true);
            assertPacksTo("all null, size " + size, isNull);
        }
    }

    @Test
    public void allElementsValid() {
        // No null ever arrives, so bytes() has to synthesize the all-valid bitmap from the running count. This is the
        // path BooleanChunkWriter takes for a payload of all-TRUE values.
        for (final int size : SIZES) {
            assertPacksTo("all valid, size " + size, new boolean[size]);
        }
    }

    @Test
    public void firstNullAtEveryPosition() {
        final int size = 200;
        for (int nullAt = 0; nullAt < size; ++nullAt) {
            final boolean[] isNull = new boolean[size];
            isNull[nullAt] = true;
            assertPacksTo("single null at " + nullAt, isNull);
        }
    }

    @Test
    public void firstValidAtEveryPosition() {
        final int size = 200;
        for (int validAt = 0; validAt < size; ++validAt) {
            final boolean[] isNull = new boolean[size];
            Arrays.fill(isNull, true);
            isNull[validAt] = false;
            assertPacksTo("single valid at " + validAt, isNull);
        }
    }

    @Test
    public void validRunThenNullsFromBoundary() {
        // Valid up to the pivot, null from there on: the first null lands at the pivot, so allocate() must back-fill
        // the words already passed with all-ones.
        final int size = 200;
        for (final int pivot : BOUNDARY_POSITIONS) {
            final boolean[] isNull = new boolean[size];
            for (int ii = pivot; ii < size; ++ii) {
                isNull[ii] = true;
            }
            assertPacksTo("valid then null from " + pivot, isNull);
        }
    }

    @Test
    public void nullRunThenValidFromBoundary() {
        // Null up to the pivot, valid from there on: the first valid element lands at the pivot.
        final int size = 200;
        for (final int pivot : BOUNDARY_POSITIONS) {
            final boolean[] isNull = new boolean[size];
            for (int ii = 0; ii < pivot && ii < size; ++ii) {
                isNull[ii] = true;
            }
            assertPacksTo("null then valid from " + pivot, isNull);
        }
    }

    @Test
    public void singleNullOrValidRunSpanningEachBoundary() {
        // A short run placed so that it straddles a word boundary, in both polarities.
        final int size = 200;
        for (final int start : BOUNDARY_POSITIONS) {
            for (final int length : new int[] {1, 2, 3, 64, 65}) {
                if (start + length > size) {
                    continue;
                }
                final boolean[] nullRun = new boolean[size];
                for (int ii = start; ii < start + length; ++ii) {
                    nullRun[ii] = true;
                }
                assertPacksTo("null run [" + start + "," + (start + length) + ")", nullRun);

                final boolean[] validRun = new boolean[size];
                Arrays.fill(validRun, true);
                for (int ii = start; ii < start + length; ++ii) {
                    validRun[ii] = false;
                }
                assertPacksTo("valid run [" + start + "," + (start + length) + ")", validRun);
            }
        }
    }

    @Test
    public void trailingBitsPastLastElementAreClear() {
        // The wire format leaves the tail of the final word undefined, but we must keep emitting zeros there: the
        // bytes are compared against other Barrage implementations byte for byte.
        for (final int size : SIZES) {
            if ((size & 63) == 0) {
                continue;
            }
            for (final boolean fillNull : new boolean[] {false, true}) {
                final boolean[] isNull = new boolean[size];
                Arrays.fill(isNull, fillNull);
                final BaseChunkWriter.ValidityBuffer validity = new BaseChunkWriter.ValidityBuffer(size);
                for (final boolean elementIsNull : isNull) {
                    validity.setNextIsNull(elementIsNull);
                }
                final byte[] bytes = validity.bytes();
                for (int bit = size; bit < bytes.length * 8; ++bit) {
                    assertThat((bytes[bit / 8] >> (bit & 7)) & 1)
                            .as("size %d fillNull %b: bit %d past the last element", size, fillNull, bit)
                            .isEqualTo(0);
                }
            }
        }
    }

    @Test
    public void setNextAreNullMatchesIndividualCalls() {
        // NullChunkWriter reports a whole column of nulls at once; that shortcut has to land on the same bytes as the
        // element-at-a-time path. A suffix follows the bulk run so that any drift in the bulk position bookkeeping
        // shows up as misplaced bits: within the run itself no bit is ever set, which hides such drift.
        final int suffixLength = 70;
        // A prefix of only valid elements leaves the buffer unmaterialized when the bulk run starts; a prefix that
        // already contains a null means the bulk run must append to an existing buffer rather than rebuild it.
        for (final boolean prefixHasNull : new boolean[] {false, true}) {
            for (final int prefixLength : new int[] {0, 1, 62, 63, 64, 65}) {
                for (final int nullRun : new int[] {0, 1, 2, 63, 64, 65, 128}) {
                    final int size = prefixLength + nullRun + suffixLength;
                    final boolean[] isNull = new boolean[size];
                    if (prefixHasNull && prefixLength > 0) {
                        // one null early in the prefix, so the buffer is already materialized
                        isNull[0] = true;
                    }
                    for (int ii = prefixLength; ii < prefixLength + nullRun; ++ii) {
                        isNull[ii] = true;
                    }
                    for (int ii = 0; ii < suffixLength; ++ii) {
                        isNull[prefixLength + nullRun + ii] = ii % 3 == 0;
                    }

                    final BaseChunkWriter.ValidityBuffer bulk = new BaseChunkWriter.ValidityBuffer(size);
                    for (int ii = 0; ii < prefixLength; ++ii) {
                        bulk.setNextIsNull(isNull[ii]);
                    }
                    bulk.setNextAreNull(nullRun);
                    for (int ii = prefixLength + nullRun; ii < size; ++ii) {
                        bulk.setNextIsNull(isNull[ii]);
                    }

                    final String description = prefixLength + " prefix (hasNull " + prefixHasNull
                            + "), setNextAreNull(" + nullRun + "), then " + suffixLength + " mixed";
                    assertThat(bulk.nullCount()).as("%s: nullCount", description).isEqualTo(countNulls(isNull));
                    assertThat(bulk.bytes()).as("%s: bitmap", description).containsExactly(expectedBitmap(isNull));
                }
            }
        }
    }

    @Test
    public void bytesIsRepeatableAndIndependentOfNullCountOrder() {
        final boolean[] isNull = new boolean[133];
        for (int ii = 0; ii < isNull.length; ++ii) {
            isNull[ii] = ii == 63 || ii == 64 || ii >= 130;
        }
        final byte[] expected = expectedBitmap(isNull);

        // bytes() first, then nullCount()
        final BaseChunkWriter.ValidityBuffer bytesFirst = new BaseChunkWriter.ValidityBuffer(isNull.length);
        for (final boolean elementIsNull : isNull) {
            bytesFirst.setNextIsNull(elementIsNull);
        }
        assertThat(bytesFirst.bytes()).as("bytes() before nullCount()").containsExactly(expected);
        assertThat(bytesFirst.nullCount()).as("nullCount() after bytes()").isEqualTo(countNulls(isNull));
        assertThat(bytesFirst.bytes()).as("bytes() called twice").containsExactly(expected);

        // and the production order, nullCount() first
        assertPacksTo("nullCount() before bytes()", isNull);
    }

    @Test
    public void randomPatterns() {
        final Random random = new Random(0xBA5EBA11L);
        for (final int size : SIZES) {
            for (int trial = 0; trial < 20; ++trial) {
                final boolean[] isNull = new boolean[size];
                for (int ii = 0; ii < size; ++ii) {
                    isNull[ii] = random.nextInt(4) == 0;
                }
                assertPacksTo("random size " + size + " trial " + trial, isNull);
            }
        }
    }
}

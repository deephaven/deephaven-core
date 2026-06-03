//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharColumnBinarySearchKernelTest and run "./gradlew replicateRegionAndRegionedSourceTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.LongChunkColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.compare.LongComparisons;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ParallelTest.class)
public class LongColumnBinarySearchKernelTest {
    private static final int CHUNK_SIZE = 4096;

    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static List<Long> makeSortedData(int size, Random rnd) {
        final List<Long> data = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            data.add((long) rnd.nextInt());
        }
        data.sort(LongComparisons::compare);
        return data;
    }

    private static List<Long> findAbsentValues(List<Long> sortedData, int num, int maxFailures, Random rnd) {
        final List<Long> missingValues = new ArrayList<>();
        int numFailedLookups = 0;
        while (missingValues.size() < num && numFailedLookups < maxFailures) {
            final long value = (long) rnd.nextInt();
            if (value == NULL_LONG
                    || Collections.binarySearch(sortedData, value, LongComparisons::compare) >= 0) {
                numFailedLookups++;
                continue;
            }
            missingValues.add(value);
        }
        return missingValues;
    }

    /**
     * Helper method to create a ChunkColumnSource from a list of values, populated in chunks of size CHUNK_SIZE.
     */
    private static LongChunkColumnSource makeChunkColumnSource(@NotNull final List<Long> values) {
        final LongChunkColumnSource source = new LongChunkColumnSource();

        // Add chunks of CHUNK_SIZE to the source
        for (int i = 0; i < values.size(); i += CHUNK_SIZE) {
            final int chunkEnd = Math.min(i + CHUNK_SIZE, values.size());
            final int chunkSize = chunkEnd - i;

            // Create a native array and fill it with data
            final long[] array = new long[chunkSize];
            for (int j = 0; j < chunkSize; j++) {
                array[j] = values.get(i + j);
            }

            // Wrap the array in a LongChunk and add it to the source
            final LongChunk<Values> chunk = LongChunk.chunkWrap(array);
            source.addChunk(chunk);
        }

        return source;
    }

    private void randomizedTestRunner(
            int size, int seed, boolean inverted) {

        final Random rnd = new Random(seed);
        final List<Long> sortedData = makeSortedData(size, rnd);
        final List<Long> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final LongChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, size - 1);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        try {
            for (int ii = 0; ii < size; ++ii) {
                final long value = data.get(ii);

                // Test match search and min/max search give the same results for this value.
                try (final RowSet matchRs = LongColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new Long[] {value}, false);
                        final RowSet minMaxRs = LongColumnBinarySearchKernel.binarySearchMinMax(
                                source, selection, sortColumn, value, value, true, true, false)) {

                    // Ensure match search and min/max search give the same results.
                    assertEquals(matchRs, minMaxRs);
                    try (final RowSet minusRs = matchRs.minus(selection)) {
                        // Ensure no extra rows are returned.
                        assertEquals(0, minusRs.size());
                    }
                    assertTrue("Expected to find " + ii, matchRs.containsRange(ii, ii));
                }
            }

            // Test negative lookups
            final List<Long> missingValues =
                    findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
            for (Long missingValue : missingValues) {
                try (final RowSet valuesFound = LongColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new Long[] {missingValue}, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = LongColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = LongColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, true, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = LongColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, true, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = LongColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, true, true, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    @Test
    public void testRandomizedDataFullRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, false);
        }
    }

    @Test
    public void testInvertedRandomizedDataFullRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, true);
        }
    }

    @Test
    public void testBinSearchMaxRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Long> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final long maxValue = (long) rnd.nextInt();

                maxTestRunner(data, false, maxValue, true);
                maxTestRunner(data, false, maxValue, false);

                maxTestRunner(data, true, maxValue, true);
                maxTestRunner(data, true, maxValue, false);
            }
        }
    }

    @Test
    public void testBinSearchMinRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Long> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final long minValue = (long) rnd.nextInt();

                minTestRunner(data, false, minValue, true);
                minTestRunner(data, false, minValue, false);

                minTestRunner(data, true, minValue, true);
                minTestRunner(data, true, minValue, false);
            }
        }
    }

    @Test
    public void testBinSearchMinMaxRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Long> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final long minCandidate = (long) rnd.nextInt();
                final long maxCandidate = (long) rnd.nextInt();

                final long minValue = (long) Math.min(minCandidate, maxCandidate);
                final long maxValue = (long) Math.max(minCandidate, maxCandidate);

                // Test all combinations of inverted/inclusive/exclusive min/max.
                minMaxTestRunner(data, false, minValue, true, maxValue, true);
                minMaxTestRunner(data, false, minValue, true, maxValue, false);
                minMaxTestRunner(data, false, minValue, false, maxValue, true);
                minMaxTestRunner(data, false, minValue, false, maxValue, false);

                minMaxTestRunner(data, true, minValue, true, maxValue, true);
                minMaxTestRunner(data, true, minValue, true, maxValue, false);
                minMaxTestRunner(data, true, minValue, false, maxValue, true);
                minMaxTestRunner(data, true, minValue, false, maxValue, false);
            }
        }
    }

    @Test
    public void testBoundsAscending() {
        // data: [1, 2, 3, 3, 3, 5, 5, 5] at positions 0-7
        final List<Long> data = List.of(
                (long) 1, (long) 2, (long) 3, (long) 3, (long) 3,
                (long) 5, (long) 5, (long) 5);
        final LongChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);
        final long lastPos = data.size() - 1;

        try {
            // lowerBound: 4 is absent; exclusive and inclusive both encode insertion point 5 as -(5+1)=-6
            assertEquals(-6, LongColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (long) 4, false, false));
            assertEquals(-6, LongColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (long) 4, true, false));
            // lowerBound: 5 is present but exclusive (value > 5) finds nothing; past-end insertion point 8 -> -(8+1)=-9
            assertEquals(-9, LongColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (long) 5, false, false));
            // lowerBound: 5 is present; inclusive search returns first occurrence at position 5
            assertEquals(5, LongColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (long) 5, true, false));

            // upperBound: 4 is absent; exclusive and inclusive both encode insertion point 5 as -(5+1)=-6
            // inserting 4 at pos 5 preserves sort order
            assertEquals(-6, LongColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (long) 4, false, false));
            // inserting 4 at pos 5 preserves sort order
            assertEquals(-6, LongColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (long) 4, true, false));
            // upperBound: 1 is present but exclusive (value < 1) finds nothing; before-start insertion point 0 ->
            // -(0+1)=-1
            // inserting 1 at pos 0 preserves sort order
            assertEquals(-1, LongColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (long) 1, false, false));
            // upperBound: 5 is present; inclusive search returns last occurrence at position 7
            // inserting 5 at pos 7 preserves sort order
            assertEquals(7, LongColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (long) 5, true, false));
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    @Test
    public void testBoundsDescending() {
        // data: [5, 5, 5, 3, 3, 3, 2, 1] at positions 0-7 (reverse of testBoundsAscending)
        final List<Long> data = List.of(
                (long) 5, (long) 5, (long) 5, (long) 3, (long) 3, (long) 3,
                (long) 2, (long) 1);
        final LongChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);
        final long lastPos = data.size() - 1;

        try {
            // lowerBound: 4 is absent; exclusive and inclusive both encode insertion point 3 as -(3+1)=-4
            assertEquals(-4, LongColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (long) 4, false, false));
            assertEquals(-4, LongColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (long) 4, true, false));
            // lowerBound: 1 is present but exclusive (value < 1) finds nothing; past-end insertion point 8 -> -(8+1)=-9
            assertEquals(-9, LongColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (long) 1, false, false));
            // lowerBound: 1 is present; inclusive search returns first (leftmost) occurrence at position 7
            assertEquals(7, LongColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (long) 1, true, false));

            // upperBound: 4 is absent; exclusive and inclusive both encode insertion point 3 as -(3+1)=-4
            assertEquals(-4, LongColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (long) 4, false, false));
            assertEquals(-4, LongColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (long) 4, true, false));
            // upperBound: 5 is present but exclusive (value > 5) finds nothing; before-start insertion point 0 ->
            // -(0+1)=-1
            assertEquals(-1, LongColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (long) 5, false, false));
            // upperBound: 5 is present; inclusive search returns last (rightmost) occurrence at position 2
            assertEquals(2, LongColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (long) 5, true, false));
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    // NOTE: missing 3 and 7 to create gaps in the data.
    private static final List<Long> GAPS_DATA = List.of(
            (long) 0, // position 0
            (long) 1, // position 1
            (long) 2, // position 2
            (long) 4, // position 3
            (long) 5, // position 4
            (long) 6, // position 5
            (long) 8, // position 6
            (long) 9); // position 7

    @Test
    public void testBinSearchWithGaps() {
        binSearchWithGapsHelper(false);
    }

    @Test
    public void testBinSearchWithGapsInverted() {
        binSearchWithGapsHelper(true);
    }

    private void binSearchWithGapsHelper(boolean inverted) {
        // From 0 to 9
        binSearchWithGaps(inverted, (long) 0, true, (long) 9, true, 8, 0, 7);
        binSearchWithGaps(inverted, (long) 0, true, (long) 9, false, 7, 0, 6);
        binSearchWithGaps(inverted, (long) 0, false, (long) 9, true, 7, 1, 7);
        binSearchWithGaps(inverted, (long) 0, false, (long) 9, false, 6, 1, 6);
        // From first to 3
        binSearchWithGaps(inverted, (long) 0, true, (long) 3, true, 3, 0, 2);
        binSearchWithGaps(inverted, (long) 0, true, (long) 3, false, 3, 0, 2);
        binSearchWithGaps(inverted, (long) 0, false, (long) 3, true, 2, 1, 2);
        binSearchWithGaps(inverted, (long) 0, false, (long) 3, false, 2, 1, 2);
        // From first to 7
        binSearchWithGaps(inverted, (long) 0, true, (long) 7, true, 6, 0, 5);
        binSearchWithGaps(inverted, (long) 0, true, (long) 7, false, 6, 0, 5);
        binSearchWithGaps(inverted, (long) 0, false, (long) 7, true, 5, 1, 5);
        binSearchWithGaps(inverted, (long) 0, false, (long) 7, false, 5, 1, 5);
        // From 3 to last
        binSearchWithGaps(inverted, (long) 3, true, (long) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (long) 3, true, (long) 9, false, 4, 3, 6);
        binSearchWithGaps(inverted, (long) 3, false, (long) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (long) 3, false, (long) 9, false, 4, 3, 6);
        // From 7 to last
        binSearchWithGaps(inverted, (long) 7, true, (long) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (long) 7, true, (long) 9, false, 1, 6, 6);
        binSearchWithGaps(inverted, (long) 7, false, (long) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (long) 7, false, (long) 9, false, 1, 6, 6);
        // From 3 to 7
        binSearchWithGaps(inverted, (long) 3, true, (long) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (long) 3, true, (long) 7, false, 3, 3, 5);
        binSearchWithGaps(inverted, (long) 3, false, (long) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (long) 3, false, (long) 7, false, 3, 3, 5);
        // From 2 to 8
        binSearchWithGaps(inverted, (long) 2, true, (long) 8, true, 5, 2, 6);
        binSearchWithGaps(inverted, (long) 2, true, (long) 8, false, 4, 2, 5);
        binSearchWithGaps(inverted, (long) 2, false, (long) 8, true, 4, 3, 6);
        binSearchWithGaps(inverted, (long) 2, false, (long) 8, false, 3, 3, 5);
    }

    private void binSearchWithGaps(
            boolean inverted,
            long minValue, boolean minInclusive,
            long maxValue, boolean maxInclusive,
            int expectedSize, long expectedFirstPosAsc, long expectedLastPosAsc) {
        final List<Long> data;
        final SortColumn sortColumn;
        if (inverted) {
            data = new ArrayList<>(GAPS_DATA);
            Collections.reverse(data);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            data = GAPS_DATA;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }
        // Adjust the expected first and last positions for inverted case.
        final long expectedFirstPos =
                inverted ? data.size() - 1 - expectedLastPosAsc : expectedFirstPosAsc;
        final long expectedLastPos =
                inverted ? data.size() - 1 - expectedFirstPosAsc : expectedLastPosAsc;

        final LongChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);

        try {
            try (final RowSet result = LongColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn, minValue, maxValue, minInclusive, maxInclusive, false)) {
                assertEquals(expectedSize, result.size());
                assertEquals(expectedFirstPos, result.firstRowKey());
                assertEquals(expectedLastPos, result.lastRowKey());
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void minMaxTestRunner(
            List<Long> data,
            final boolean inverted,
            final long minValue,
            final boolean minInclusive,
            final long maxValue,
            final boolean maxInclusive) {

        final List<Long> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final LongChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = LongColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn, minValue, maxValue, minInclusive, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final long value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(LongComparisons.compare(value, minValue) >= 0);
                    } else {
                        assertTrue(LongComparisons.compare(value, minValue) > 0);
                    }
                    if (maxInclusive) {
                        assertTrue(LongComparisons.compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(LongComparisons.compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void minTestRunner(
            List<Long> data,
            final boolean inverted,
            final long minValue,
            final boolean minInclusive) {

        final List<Long> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final LongChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = LongColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn, minValue, minInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final long value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(LongComparisons.compare(value, minValue) >= 0);
                    } else {
                        assertTrue(LongComparisons.compare(value, minValue) > 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void maxTestRunner(
            List<Long> data,
            final boolean inverted,
            final long maxValue,
            final boolean maxInclusive) {

        final List<Long> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final LongChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = LongColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn, maxValue, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final long value = dataToUse.get((int) rowKey);
                    if (maxInclusive) {
                        assertTrue(LongComparisons.compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(LongComparisons.compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }
}


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
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.IntChunkColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.compare.IntComparisons;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ParallelTest.class)
public class IntColumnBinarySearchKernelTest {
    private static final int CHUNK_SIZE = 4096;

    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static List<Integer> makeSortedData(int size, Random rnd) {
        final List<Integer> data = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            data.add((int) rnd.nextInt());
        }
        data.sort(IntComparisons::compare);
        return data;
    }

    private static List<Integer> findAbsentValues(List<Integer> sortedData, int num, int maxFailures, Random rnd) {
        final List<Integer> missingValues = new ArrayList<>();
        int numFailedLookups = 0;
        while (missingValues.size() < num && numFailedLookups < maxFailures) {
            final int value = (int) rnd.nextInt();
            if (value == NULL_INT
                    || Collections.binarySearch(sortedData, value, IntComparisons::compare) >= 0) {
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
    private static IntChunkColumnSource makeChunkColumnSource(@NotNull final List<Integer> values) {
        final IntChunkColumnSource source = new IntChunkColumnSource();

        // Add chunks of CHUNK_SIZE to the source
        for (int i = 0; i < values.size(); i += CHUNK_SIZE) {
            final int chunkEnd = Math.min(i + CHUNK_SIZE, values.size());
            final int chunkSize = chunkEnd - i;

            // Create a native array and fill it with data
            final int[] array = new int[chunkSize];
            for (int j = 0; j < chunkSize; j++) {
                array[j] = values.get(i + j);
            }

            // Wrap the array in a IntChunk and add it to the source
            final IntChunk<Values> chunk = IntChunk.chunkWrap(array);
            source.addChunk(chunk);
        }

        return source;
    }

    private void randomizedTestRunner(
            int size, int seed, boolean inverted) {

        final Random rnd = new Random(seed);
        final List<Integer> sortedData = makeSortedData(size, rnd);
        final List<Integer> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final IntChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, size - 1);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        try {
            for (int ii = 0; ii < size; ++ii) {
                final int value = data.get(ii);

                // Test match search and min/max search give the same results for this value.
                try (final RowSet matchRs = IntColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new Integer[] {value}, false);
                        final RowSet minMaxRs = IntColumnBinarySearchKernel.binarySearchMinMax(
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
            final List<Integer> missingValues =
                    findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
            for (Integer missingValue : missingValues) {
                try (final RowSet valuesFound = IntColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new Integer[] {missingValue}, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = IntColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = IntColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, true, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = IntColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, true, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = IntColumnBinarySearchKernel.binarySearchMinMax(
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
            final List<Integer> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final int maxValue = (int) rnd.nextInt();

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
            final List<Integer> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final int minValue = (int) rnd.nextInt();

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
            final List<Integer> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final int minCandidate = (int) rnd.nextInt();
                final int maxCandidate = (int) rnd.nextInt();

                final int minValue = (int) Math.min(minCandidate, maxCandidate);
                final int maxValue = (int) Math.max(minCandidate, maxCandidate);

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
        final List<Integer> data = List.of(
                (int) 1, (int) 2, (int) 3, (int) 3, (int) 3,
                (int) 5, (int) 5, (int) 5);
        final IntChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);
        final long lastPos = data.size() - 1;

        try {
            // lowerBound: 4 is absent; exclusive and inclusive both encode insertion point 5 as -(5+1)=-6
            assertEquals(-6, IntColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (int) 4, false, false));
            assertEquals(-6, IntColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (int) 4, true, false));
            // lowerBound: 5 is present but exclusive (value > 5) finds nothing; past-end insertion point 8 -> -(8+1)=-9
            assertEquals(-9, IntColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (int) 5, false, false));
            // lowerBound: 5 is present; inclusive search returns first occurrence at position 5
            assertEquals(5, IntColumnBinarySearchKernel.lowerBoundAscending(
                    source, selection, 0, lastPos, (int) 5, true, false));

            // upperBound: 4 is absent; exclusive and inclusive both encode insertion point 5 as -(5+1)=-6
            // inserting 4 at pos 5 preserves sort order
            assertEquals(-6, IntColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (int) 4, false, false));
            // inserting 4 at pos 5 preserves sort order
            assertEquals(-6, IntColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (int) 4, true, false));
            // upperBound: 1 is present but exclusive (value < 1) finds nothing; before-start insertion point 0 ->
            // -(0+1)=-1
            // inserting 1 at pos 0 preserves sort order
            assertEquals(-1, IntColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (int) 1, false, false));
            // upperBound: 5 is present; inclusive search returns last occurrence at position 7
            // inserting 5 at pos 7 preserves sort order
            assertEquals(7, IntColumnBinarySearchKernel.upperBoundAscending(
                    source, selection, 0, lastPos, (int) 5, true, false));
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    @Test
    public void testBoundsDescending() {
        // data: [5, 5, 5, 3, 3, 3, 2, 1] at positions 0-7 (reverse of testBoundsAscending)
        final List<Integer> data = List.of(
                (int) 5, (int) 5, (int) 5, (int) 3, (int) 3, (int) 3,
                (int) 2, (int) 1);
        final IntChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);
        final long lastPos = data.size() - 1;

        try {
            // lowerBound: 4 is absent; exclusive and inclusive both encode insertion point 3 as -(3+1)=-4
            assertEquals(-4, IntColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (int) 4, false, false));
            assertEquals(-4, IntColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (int) 4, true, false));
            // lowerBound: 1 is present but exclusive (value < 1) finds nothing; past-end insertion point 8 -> -(8+1)=-9
            assertEquals(-9, IntColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (int) 1, false, false));
            // lowerBound: 1 is present; inclusive search returns first (leftmost) occurrence at position 7
            assertEquals(7, IntColumnBinarySearchKernel.lowerBoundDescending(
                    source, selection, 0, lastPos, (int) 1, true, false));

            // upperBound: 4 is absent; exclusive and inclusive both encode insertion point 3 as -(3+1)=-4
            assertEquals(-4, IntColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (int) 4, false, false));
            assertEquals(-4, IntColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (int) 4, true, false));
            // upperBound: 5 is present but exclusive (value > 5) finds nothing; before-start insertion point 0 ->
            // -(0+1)=-1
            assertEquals(-1, IntColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (int) 5, false, false));
            // upperBound: 5 is present; inclusive search returns last (rightmost) occurrence at position 2
            assertEquals(2, IntColumnBinarySearchKernel.upperBoundDescending(
                    source, selection, 0, lastPos, (int) 5, true, false));
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    // NOTE: missing 3 and 7 to create gaps in the data.
    private static final List<Integer> GAPS_DATA = List.of(
            (int) 0, // position 0
            (int) 1, // position 1
            (int) 2, // position 2
            (int) 4, // position 3
            (int) 5, // position 4
            (int) 6, // position 5
            (int) 8, // position 6
            (int) 9); // position 7

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
        binSearchWithGaps(inverted, (int) 0, true, (int) 9, true, 8, 0, 7);
        binSearchWithGaps(inverted, (int) 0, true, (int) 9, false, 7, 0, 6);
        binSearchWithGaps(inverted, (int) 0, false, (int) 9, true, 7, 1, 7);
        binSearchWithGaps(inverted, (int) 0, false, (int) 9, false, 6, 1, 6);
        // From first to 3
        binSearchWithGaps(inverted, (int) 0, true, (int) 3, true, 3, 0, 2);
        binSearchWithGaps(inverted, (int) 0, true, (int) 3, false, 3, 0, 2);
        binSearchWithGaps(inverted, (int) 0, false, (int) 3, true, 2, 1, 2);
        binSearchWithGaps(inverted, (int) 0, false, (int) 3, false, 2, 1, 2);
        // From first to 7
        binSearchWithGaps(inverted, (int) 0, true, (int) 7, true, 6, 0, 5);
        binSearchWithGaps(inverted, (int) 0, true, (int) 7, false, 6, 0, 5);
        binSearchWithGaps(inverted, (int) 0, false, (int) 7, true, 5, 1, 5);
        binSearchWithGaps(inverted, (int) 0, false, (int) 7, false, 5, 1, 5);
        // From 3 to last
        binSearchWithGaps(inverted, (int) 3, true, (int) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (int) 3, true, (int) 9, false, 4, 3, 6);
        binSearchWithGaps(inverted, (int) 3, false, (int) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (int) 3, false, (int) 9, false, 4, 3, 6);
        // From 7 to last
        binSearchWithGaps(inverted, (int) 7, true, (int) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (int) 7, true, (int) 9, false, 1, 6, 6);
        binSearchWithGaps(inverted, (int) 7, false, (int) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (int) 7, false, (int) 9, false, 1, 6, 6);
        // From 3 to 7
        binSearchWithGaps(inverted, (int) 3, true, (int) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (int) 3, true, (int) 7, false, 3, 3, 5);
        binSearchWithGaps(inverted, (int) 3, false, (int) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (int) 3, false, (int) 7, false, 3, 3, 5);
        // From 2 to 8
        binSearchWithGaps(inverted, (int) 2, true, (int) 8, true, 5, 2, 6);
        binSearchWithGaps(inverted, (int) 2, true, (int) 8, false, 4, 2, 5);
        binSearchWithGaps(inverted, (int) 2, false, (int) 8, true, 4, 3, 6);
        binSearchWithGaps(inverted, (int) 2, false, (int) 8, false, 3, 3, 5);
    }

    private void binSearchWithGaps(
            boolean inverted,
            int minValue, boolean minInclusive,
            int maxValue, boolean maxInclusive,
            int expectedSize, long expectedFirstPosAsc, long expectedLastPosAsc) {
        final List<Integer> data;
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

        final IntChunkColumnSource source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);

        try {
            try (final RowSet result = IntColumnBinarySearchKernel.binarySearchMinMax(
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
            List<Integer> data,
            final boolean inverted,
            final int minValue,
            final boolean minInclusive,
            final int maxValue,
            final boolean maxInclusive) {

        final List<Integer> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final IntChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = IntColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn, minValue, maxValue, minInclusive, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final int value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(IntComparisons.compare(value, minValue) >= 0);
                    } else {
                        assertTrue(IntComparisons.compare(value, minValue) > 0);
                    }
                    if (maxInclusive) {
                        assertTrue(IntComparisons.compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(IntComparisons.compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void minTestRunner(
            List<Integer> data,
            final boolean inverted,
            final int minValue,
            final boolean minInclusive) {

        final List<Integer> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final IntChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = IntColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn, minValue, minInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final int value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(IntComparisons.compare(value, minValue) >= 0);
                    } else {
                        assertTrue(IntComparisons.compare(value, minValue) > 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void maxTestRunner(
            List<Integer> data,
            final boolean inverted,
            final int maxValue,
            final boolean maxInclusive) {

        final List<Integer> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final IntChunkColumnSource source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = IntColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn, maxValue, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final int value = dataToUse.get((int) rowKey);
                    if (maxInclusive) {
                        assertTrue(IntComparisons.compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(IntComparisons.compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }
}


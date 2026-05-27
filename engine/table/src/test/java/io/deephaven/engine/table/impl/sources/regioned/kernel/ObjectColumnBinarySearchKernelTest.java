//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ObjectChunkColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.ParallelTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ParallelTest.class)
public class ObjectColumnBinarySearchKernelTest {
    private static final int CHUNK_SIZE = 4096;

    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static int STRING_LENGTH = 3;
    private static final char[] ALPHA_NUMERIC_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private static String makeString(Random random, int length) {
        if (length <= 0) {
            return "";
        }

        char[] buffer = new char[length];

        for (int i = 0; i < length; i++) {
            buffer[i] = ALPHA_NUMERIC_CHARS[random.nextInt(ALPHA_NUMERIC_CHARS.length)];
        }

        return new String(buffer);
    }

    private static List<String> makeSortedData(int size, Random rnd) {
        final List<String> data = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            data.add(makeString(rnd, STRING_LENGTH));
        }
        data.sort(Comparator.naturalOrder());
        return data;
    }

    private static List<String> findAbsentValues(List<String> sortedData, int num, int maxFailures, Random rnd) {
        final List<String> missingValues = new ArrayList<>();
        int numFailedLookups = 0;
        while (missingValues.size() < num && numFailedLookups < maxFailures) {
            final String value = makeString(rnd, STRING_LENGTH);
            if (value == null
                    || Collections.binarySearch(sortedData, value, Comparator.naturalOrder()) >= 0) {
                numFailedLookups++;
                continue;
            }
            missingValues.add(value);
        }
        return missingValues;
    }

    /**
     * Helper method to create an ObjectChunkColumnSource from a list of objects, populated in chunks
     * of size CHUNK_SIZE.
     */
    private static ObjectChunkColumnSource<String> makeChunkColumnSource(@NotNull final List<String> values) {
        @SuppressWarnings("unchecked") final ObjectChunkColumnSource<String> source =
                (ObjectChunkColumnSource<String>)ChunkColumnSource.make(ChunkType.Object, String.class);

        // Add chunks of CHUNK_SIZE to the source
        for (int i = 0; i < values.size(); i += CHUNK_SIZE) {
            final int chunkEnd = Math.min(i + CHUNK_SIZE, values.size());
            final int chunkSize = chunkEnd - i;

            // Create a native array and fill it with data
            final String[] array = new String[chunkSize];
            for (int j = 0; j < chunkSize; j++) {
                array[j] = values.get(i + j);
            }

            // Wrap the array in an ObjectChunk and add it to the source
            final ObjectChunk<String, Values> chunk = ObjectChunk.chunkWrap(array);
            source.addChunk(chunk);
        }

        return source;
    }

    private void randomizedTestRunner(
            int size, int seed, boolean inverted) {

        final Random rnd = new Random(seed);
        final List<String> sortedData = makeSortedData(size, rnd);
        final List<String> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final ObjectChunkColumnSource<String> source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, size - 1);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        try {
            for (int ii = 0; ii < size; ++ii) {
                final String value = data.get(ii);

                // Test match search and min/max search give the same results for this value.
                try (final RowSet matchRs = ObjectColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new String[] {value}, false);
                     final RowSet minMaxRs = ObjectColumnBinarySearchKernel.binarySearchMinMax(
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
            final List<String> missingValues =
                    findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
            for (String missingValue : missingValues) {
                try (final RowSet valuesFound = ObjectColumnBinarySearchKernel.binarySearchMatch(
                        source, selection, sortColumn, new String[] {missingValue}, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = ObjectColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = ObjectColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, true, false, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = ObjectColumnBinarySearchKernel.binarySearchMinMax(
                        source, selection, sortColumn, missingValue, missingValue, false, true, false)) {
                    assertTrue(valuesFound.isEmpty());
                }
                try (final RowSet valuesFound = ObjectColumnBinarySearchKernel.binarySearchMinMax(
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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String maxValue = makeString(rnd, STRING_LENGTH);

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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String minValue = makeString(rnd, STRING_LENGTH);

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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String minCandidate = makeString(rnd, STRING_LENGTH);
                final String maxCandidate = makeString(rnd, STRING_LENGTH);

                final String minValue = Collections.min(List.of(minCandidate, maxCandidate));
                final String maxValue = Collections.max(List.of(minCandidate, maxCandidate));

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

    // NOTE: missing "3" and "7" to create gaps in the data.
    private static final List<String> GAPS_DATA = List.of(
            "0", // position 0
            "1", // position 1
            "2", // position 2
            "4", // position 3
            "5", // position 4
            "6", // position 5
            "8", // position 6
            "9"); // position 7

    @Test
    public void testBinSearchWithGaps() {
        binSearchWithGapsHelper(false);
    }

    @Test
    public void testBinSearchWithGapsInverted() {
        binSearchWithGapsHelper(true);
    }

    private void binSearchWithGapsHelper(boolean inverted) {
        // From "0" to "9"
        binSearchWithGaps(inverted, "0", true, "9", true, 8, 0, 7);
        binSearchWithGaps(inverted, "0", true, "9", false, 7, 0, 6);
        binSearchWithGaps(inverted, "0", false, "9", true, 7, 1, 7);
        binSearchWithGaps(inverted, "0", false, "9", false, 6, 1, 6);
        // From first to "3"
        binSearchWithGaps(inverted, "0", true, "3", true, 3, 0, 2);
        binSearchWithGaps(inverted, "0", true, "3", false, 3, 0, 2);
        binSearchWithGaps(inverted, "0", false, "3", true, 2, 1, 2);
        binSearchWithGaps(inverted, "0", false, "3", false, 2, 1, 2);
        // From first to "7"
        binSearchWithGaps(inverted, "0", true, "7", true, 6, 0, 5);
        binSearchWithGaps(inverted, "0", true, "7", false, 6, 0, 5);
        binSearchWithGaps(inverted, "0", false, "7", true, 5, 1, 5);
        binSearchWithGaps(inverted, "0", false, "7", false, 5, 1, 5);
        // From "3" to last
        binSearchWithGaps(inverted, "3", true, "9", true, 5, 3, 7);
        binSearchWithGaps(inverted, "3", true, "9", false, 4, 3, 6);
        binSearchWithGaps(inverted, "3", false, "9", true, 5, 3, 7);
        binSearchWithGaps(inverted, "3", false, "9", false, 4, 3, 6);
        // From "7" to last
        binSearchWithGaps(inverted, "7", true, "9", true, 2, 6, 7);
        binSearchWithGaps(inverted, "7", true, "9", false, 1, 6, 6);
        binSearchWithGaps(inverted, "7", false, "9", true, 2, 6, 7);
        binSearchWithGaps(inverted, "7", false, "9", false, 1, 6, 6);
        // From "3" to "7"
        binSearchWithGaps(inverted, "3", true, "7", true, 3, 3, 5);
        binSearchWithGaps(inverted, "3", true, "7", false, 3, 3, 5);
        binSearchWithGaps(inverted, "3", false, "7", true, 3, 3, 5);
        binSearchWithGaps(inverted, "3", false, "7", false, 3, 3, 5);
        // From "2" to "8"
        binSearchWithGaps(inverted, "2", true, "8", true, 5, 2, 6);
        binSearchWithGaps(inverted, "2", true, "8", false, 4, 2, 5);
        binSearchWithGaps(inverted, "2", false, "8", true, 4, 3, 6);
        binSearchWithGaps(inverted, "2", false, "8", false, 3, 3, 5);
    }

    private void binSearchWithGaps(
            boolean inverted,
            String minValue, boolean minInclusive,
            String maxValue, boolean maxInclusive,
            int expectedSize, long expectedFirstPosAsc, long expectedLastPosAsc) {
        final List<String> data;
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

        final ObjectChunkColumnSource<String> source = makeChunkColumnSource(data);
        final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);

        try {
            try (final RowSet result = ObjectColumnBinarySearchKernel.binarySearchMinMax(
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
            List<String> data,
            final boolean inverted,
            final String minValue,
            final boolean minInclusive,
            final String maxValue,
            final boolean maxInclusive) {

        final List<String> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ObjectChunkColumnSource<String> source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = ObjectColumnBinarySearchKernel.binarySearchMinMax(
                    source, selection, sortColumn, minValue, maxValue, minInclusive, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final String value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) >= 0);
                    } else {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) > 0);
                    }
                    if (maxInclusive) {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void minTestRunner(
            List<String> data,
            final boolean inverted,
            final String minValue,
            final boolean minInclusive) {

        final List<String> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ObjectChunkColumnSource<String> source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = ObjectColumnBinarySearchKernel.binarySearchMin(
                    source, selection, sortColumn, minValue, minInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final String value = dataToUse.get((int) rowKey);
                    if (minInclusive) {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) >= 0);
                    } else {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) > 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }

    private void maxTestRunner(
            List<String> data,
            final boolean inverted,
            final String maxValue,
            final boolean maxInclusive) {

        final List<String> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ObjectChunkColumnSource<String> source = makeChunkColumnSource(dataToUse);
        final RowSet selection = RowSetFactory.fromRange(0, dataToUse.size() - 1);

        try {
            try (final RowSet result = ObjectColumnBinarySearchKernel.binarySearchMax(
                    source, selection, sortColumn, maxValue, maxInclusive, false)) {

                // Go through every value in the result and ensure it is within the min/max bounds.
                result.forAllRowKeys(rowKey -> {
                    // The value at the row key must be within the min/max bounds.
                    final String value = dataToUse.get((int) rowKey);
                    if (maxInclusive) {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) <= 0);
                    } else {
                        assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) < 0);
                    }
                });
            }
        } finally {
            selection.close();
            source.clear(true);
        }
    }
}


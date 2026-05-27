//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRegionBinarySearchKernelTest and run "./gradlew replicateRegionAndRegionedSourceTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionByte;
import io.deephaven.generic.region.AppendOnlyRegionAccessor;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.compare.ByteComparisons;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntToLongFunction;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static org.junit.Assert.*;

@Category(ParallelTest.class)
public class ByteRegionBinarySearchKernelTest {
    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static List<Byte> makeSortedData(int size, Random rnd) {
        final List<Byte> data = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            data.add((byte) rnd.nextInt());
        }
        data.sort(ByteComparisons::compare);
        return data;
    }

    private static List<Byte> findAbsentValues(List<Byte> sortedData, int num, int maxFailures, Random rnd) {
        final List<Byte> missingValues = new ArrayList<>();
        int numFailedLookups = 0;
        while (missingValues.size() < num && numFailedLookups < maxFailures) {
            final byte value = (byte) rnd.nextInt();
            if (value == NULL_BYTE
                    || Collections.binarySearch(sortedData, value, ByteComparisons::compare) >= 0) {
                numFailedLookups++;
                continue;
            }
            missingValues.add(value);
        }
        return missingValues;
    }

    private void randomizedTestRunner(
            int size, int seed, boolean inverted, IntToLongFunction firstKey, IntToLongFunction lastKey) {

        final Random rnd = new Random(seed);
        final List<Byte> sortedData = makeSortedData(size, rnd);
        final List<Byte> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final ColumnRegionByte<Values> region = makeColumnRegionByte(data);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        for (int ii = 0; ii < size; ++ii) {
            final byte value = data.get(ii);
            final long startRow = Math.max(0, firstKey.applyAsLong(ii));
            final long endRow = Math.min(size - 1, lastKey.applyAsLong(ii));
            // Test match search and min/max search give the same results for this value.
            try (final RowSet matchRs = ByteRegionBinarySearchKernel.binarySearchMatch(
                    region, startRow, endRow, sortColumn, new Byte[] {value});
                 final RowSet minMaxRs = ByteRegionBinarySearchKernel.binarySearchMinMax(
                         region, startRow, endRow, sortColumn, value, value, true, true)) {
                // Ensure match search and min/max search give the same results.
                assertEquals(matchRs, minMaxRs);

                // Test the results for correctness.
                if (matchRs.isNonempty()) {
                    // Ensure not returning outside row limits.
                    assertTrue(matchRs.firstRowKey() >= startRow);
                    assertTrue(matchRs.lastRowKey() <= endRow);
                }
                if (startRow <= ii && ii <= endRow) {
                    assertTrue("Expected to find " + ii, matchRs.containsRange(ii, ii));
                } else {
                    assertFalse("Not expected to find " + ii, matchRs.containsRange(ii, ii));
                }
            }
            // Ensure not found when not expected.
            try (final RowSet valuesFound = ByteRegionBinarySearchKernel.binarySearchMinMax(
                    region, startRow, endRow, sortColumn, value, value, true, false)) {
                assertTrue(valuesFound.isEmpty());
            }
            try (final RowSet valuesFound = ByteRegionBinarySearchKernel.binarySearchMinMax(
                    region, startRow, endRow, sortColumn, value, value, false, true)) {
                assertTrue(valuesFound.isEmpty());
            }
            try (final RowSet valuesFound = ByteRegionBinarySearchKernel.binarySearchMinMax(
                    region, startRow, endRow, sortColumn, value, value, false, false)) {
                assertTrue(valuesFound.isEmpty());
            }
        }

        // Test negative lookups
        final List<Byte> missingValues =
                findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
        for (Byte missingValue : missingValues) {
            final long startRow = 0;
            final long endRow = size - 1;
            try (final RowSet valuesFound = ByteRegionBinarySearchKernel.binarySearchMatch(
                    region, startRow, endRow, sortColumn, new Byte[] {missingValue})) {
                assertTrue(valuesFound.isEmpty());
            }
            try (final RowSet valuesFound = ByteRegionBinarySearchKernel.binarySearchMinMax(
                    region, startRow, endRow, sortColumn, missingValue, missingValue, true, true)) {
                assertTrue(valuesFound.isEmpty());
            }
        }
    }

    private void randomizedTestRunner(
            int size, int seed, IntToLongFunction firstKey, IntToLongFunction lastKey) {
        randomizedTestRunner(size, seed, false, firstKey, lastKey);
    }

    private void invertedRandomizedTestRunner(
            int size, int seed, IntToLongFunction firstKey, IntToLongFunction lastKey) {
        randomizedTestRunner(size, seed, true, firstKey, lastKey);
    }

    @Test
    public void testRandomizedDataFullRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> 0, i -> size);
        }
    }

    @Test
    public void testRowIsAboveRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> 0, i -> i - 1);
        }
    }

    @Test
    public void testRowUpperBoundRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> 0, i -> i);
        }
    }

    @Test
    public void testRowInLowerRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> 0, i -> i + 1);
        }
    }

    @Test
    public void testRowIsBelowRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> i + 1, i -> size);
        }
    }

    @Test
    public void testRowLowerBoundRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> i, i -> size);
        }
    }

    @Test
    public void testRowInUpperRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> i - 1, i -> size);
        }
    }

    @Test
    public void testRowIsRange() {
        for (int size : SIZES) {
            randomizedTestRunner(size, 0, i -> i, i -> i);
        }
    }

    @Test
    public void testInvertedRandomizedDataFullRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> 0, i -> size);
        }
    }

    @Test
    public void testInvertedRowIsAboveRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> 0, i -> i - 1);
        }
    }

    @Test
    public void testInvertedRowUpperBoundRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> 0, i -> i);
        }
    }

    @Test
    public void testInvertedRowInLowerRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> 0, i -> i + 1);
        }
    }

    @Test
    public void testInvertedRowIsBelowRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> i + 1, i -> size);
        }
    }

    @Test
    public void testInvertedRowLowerBoundRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> i, i -> size);
        }
    }

    @Test
    public void testInvertedRowInUpperRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> i - 1, i -> size);
        }
    }

    @Test
    public void testInvertedRowIsRange() {
        for (int size : SIZES) {
            invertedRandomizedTestRunner(size, 0, i -> i, i -> i);
        }
    }

    @Test
    public void testBinSearchMaxRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Byte> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final byte maxValue = (byte) rnd.nextInt();

                final long firstKeyCandidate = rnd.nextInt(size);
                final long lastKeyCandidate = rnd.nextInt(size);

                final long firstKey = Math.min(firstKeyCandidate, lastKeyCandidate);
                final long lastKey = Math.max(firstKeyCandidate, lastKeyCandidate);

                maxTestRunner(data, false, firstKey, lastKey, maxValue, true);
                maxTestRunner(data, false, firstKey, lastKey, maxValue, false);

                maxTestRunner(data, true, firstKey, lastKey, maxValue, true);
                maxTestRunner(data, true, firstKey, lastKey, maxValue, false);
            }
        }
    }

    @Test
    public void testBinSearchMinRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Byte> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final byte minValue = (byte) rnd.nextInt();

                final long firstKeyCandidate = rnd.nextInt(size);
                final long lastKeyCandidate = rnd.nextInt(size);

                final long firstKey = Math.min(firstKeyCandidate, lastKeyCandidate);
                final long lastKey = Math.max(firstKeyCandidate, lastKeyCandidate);

                minTestRunner(data, false, firstKey, lastKey, minValue, true);
                minTestRunner(data, false, firstKey, lastKey, minValue, false);

                minTestRunner(data, true, firstKey, lastKey, minValue, true);
                minTestRunner(data, true, firstKey, lastKey, minValue, false);
            }
        }
    }

    @Test
    public void testBinSearchMinMaxRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Byte> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final byte minCandidate = (byte) rnd.nextInt();
                final byte maxCandidate = (byte) rnd.nextInt();

                final byte minValue = (byte) Math.min(minCandidate, maxCandidate);
                final byte maxValue = (byte) Math.max(minCandidate, maxCandidate);

                final long firstKeyCandidate = rnd.nextInt(size);
                final long lastKeyCandidate = rnd.nextInt(size);

                final long firstKey = Math.min(firstKeyCandidate, lastKeyCandidate);
                final long lastKey = Math.max(firstKeyCandidate, lastKeyCandidate);

                // Test all combinations of inverted/inclusive/exclusive min/max.
                minMaxTestRunner(data, false, firstKey, lastKey, minValue, true, maxValue, true);
                minMaxTestRunner(data, false, firstKey, lastKey, minValue, true, maxValue, false);
                minMaxTestRunner(data, false, firstKey, lastKey, minValue, false, maxValue, true);
                minMaxTestRunner(data, false, firstKey, lastKey, minValue, false, maxValue, false);

                minMaxTestRunner(data, true, firstKey, lastKey, minValue, true, maxValue, true);
                minMaxTestRunner(data, true, firstKey, lastKey, minValue, true, maxValue, false);
                minMaxTestRunner(data, true, firstKey, lastKey, minValue, false, maxValue, true);
                minMaxTestRunner(data, true, firstKey, lastKey, minValue, false, maxValue, false);
            }
        }
    }

    // NOTE: missing 3 and 7 to create gaps in the data.
    private static final List<Byte> GAPS_DATA = List.of(
            (byte) 0, // row 0
            (byte) 1, // row 1
            (byte) 2, // row 2
            (byte) 4, // row 3
            (byte) 5, // row 4
            (byte) 6, // row 5
            (byte) 8, // row 6
            (byte) 9); // row 7

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
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 9, true, 8, 0, 7);
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 9, false, 7, 0, 6);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 9, true, 7, 1, 7);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 9, false, 6, 1, 6);
        // From first to 3
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 3, true, 3, 0, 2);
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 3, false, 3, 0, 2);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 3, true, 2, 1, 2);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 3, false, 2, 1, 2);
        // From first to 7
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 7, true, 6, 0, 5);
        binSearchWithGaps(inverted, (byte) 0, true, (byte) 7, false, 6, 0, 5);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 7, true, 5, 1, 5);
        binSearchWithGaps(inverted, (byte) 0, false, (byte) 7, false, 5, 1, 5);
        // From 3 to last
        binSearchWithGaps(inverted, (byte) 3, true, (byte) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (byte) 3, true, (byte) 9, false, 4, 3, 6);
        binSearchWithGaps(inverted, (byte) 3, false, (byte) 9, true, 5, 3, 7);
        binSearchWithGaps(inverted, (byte) 3, false, (byte) 9, false, 4, 3, 6);
        // From 7 to last
        binSearchWithGaps(inverted, (byte) 7, true, (byte) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (byte) 7, true, (byte) 9, false, 1, 6, 6);
        binSearchWithGaps(inverted, (byte) 7, false, (byte) 9, true, 2, 6, 7);
        binSearchWithGaps(inverted, (byte) 7, false, (byte) 9, false, 1, 6, 6);
        // From 3 to 7
        binSearchWithGaps(inverted, (byte) 3, true, (byte) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (byte) 3, true, (byte) 7, false, 3, 3, 5);
        binSearchWithGaps(inverted, (byte) 3, false, (byte) 7, true, 3, 3, 5);
        binSearchWithGaps(inverted, (byte) 3, false, (byte) 7, false, 3, 3, 5);
        // From 2 to 8
        binSearchWithGaps(inverted, (byte) 2, true, (byte) 8, true, 5, 2, 6);
        binSearchWithGaps(inverted, (byte) 2, true, (byte) 8, false, 4, 2, 5);
        binSearchWithGaps(inverted, (byte) 2, false, (byte) 8, true, 4, 3, 6);
        binSearchWithGaps(inverted, (byte) 2, false, (byte) 8, false, 3, 3, 5);
    }

    private void binSearchWithGaps(
            boolean inverted,
            byte minValue, boolean minInclusive,
            byte maxValue, boolean maxInclusive,
            int expectedSize, long expectedFirstRowAsc, long expectedLastRowAsc) {
        final List<Byte> data;
        final SortColumn sortColumn;
        if (inverted) {
            data = new ArrayList<>(GAPS_DATA);
            Collections.reverse(data);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            data = GAPS_DATA;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }
        // Adjust the expected first and last rows for inverted case.
        final long expectedFirstRow =
                inverted ? data.size() - 1 - expectedLastRowAsc : expectedFirstRowAsc;
        final long expectedLastRow =
                inverted ? data.size() - 1 - expectedFirstRowAsc : expectedLastRowAsc;

        final ColumnRegionByte<Values> region = makeColumnRegionByte(data);
        try (final RowSet result = ByteRegionBinarySearchKernel.binarySearchMinMax(
                region, 0, data.size() - 1, sortColumn,
                minValue, maxValue, minInclusive, maxInclusive)) {
            assertEquals(expectedSize, result.size());
            assertEquals(expectedFirstRow, result.firstRowKey());
            assertEquals(expectedLastRow, result.lastRowKey());
        }
    }

    private void minMaxTestRunner(
            List<Byte> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final byte minValue,
            final boolean minInclusive,
            final byte maxValue,
            final boolean maxInclusive) {

        final List<Byte> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionByte<Values> region = makeColumnRegionByte(dataToUse);

        try (final RowSet result = ByteRegionBinarySearchKernel.binarySearchMinMax(
                region, firstKey, lastKey, sortColumn, minValue, maxValue, minInclusive, maxInclusive)) {

            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final byte value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    assertTrue(ByteComparisons.compare(value, minValue) >= 0);
                } else {
                    assertTrue(ByteComparisons.compare(value, minValue) > 0);
                }
                if (maxInclusive) {
                    assertTrue(ByteComparisons.compare(value, maxValue) <= 0);
                } else {
                    assertTrue(ByteComparisons.compare(value, maxValue) < 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                assertTrue(intersection.isEmpty());
            }
        }
    }

    private void minTestRunner(
            List<Byte> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final byte minValue,
            final boolean minInclusive) {

        final List<Byte> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionByte<Values> region = makeColumnRegionByte(dataToUse);

        try (final RowSet result = ByteRegionBinarySearchKernel.binarySearchMin(
                region, firstKey, lastKey, sortColumn, minValue, minInclusive)) {
            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final byte value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    assertTrue(ByteComparisons.compare(value, minValue) >= 0);
                } else {
                    assertTrue(ByteComparisons.compare(value, minValue) > 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                assertTrue(intersection.isEmpty());
            }
        }
    }

    private void maxTestRunner(
            List<Byte> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final byte maxValue,
            final boolean maxInclusive) {

        final List<Byte> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionByte<Values> region = makeColumnRegionByte(dataToUse);

        try (final RowSet result = ByteRegionBinarySearchKernel.binarySearchMax(
                region, firstKey, lastKey, sortColumn, maxValue, maxInclusive)) {

            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final byte value = dataToUse.get((int) rowKey);
                if (maxInclusive) {
                    assertTrue(ByteComparisons.compare(value, maxValue) <= 0);
                } else {
                    assertTrue(ByteComparisons.compare(value, maxValue) < 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                assertTrue(intersection.isEmpty());
            }
        }
    }

    private static final int PAGE_SIZE = 1 << 16;

    private static ColumnRegionByte<Values> makeColumnRegionByte(@NotNull final List<Byte> values) {
        return new AppendOnlyFixedSizePageRegionByte<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, new AppendOnlyRegionAccessor<>() {
                    @Override
                    public void readChunkPage(long firstRowPosition, int minimumSize,
                            @NotNull WritableChunk<Values> destination) {
                        int finalSize = (int) Math.min(minimumSize, values.size() - firstRowPosition);
                        destination.setSize(finalSize);
                        for (int ii = 0; ii < finalSize; ++ii) {
                            destination.asWritableByteChunk().set(ii, values.get((int) firstRowPosition + ii));
                        }
                    }

                    @Override
                    public long size() {
                        return values.size();
                    }
                });
    }
}

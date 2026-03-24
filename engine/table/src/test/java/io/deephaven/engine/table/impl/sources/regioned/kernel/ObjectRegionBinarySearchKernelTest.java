//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionObject;
import io.deephaven.generic.region.AppendOnlyRegionAccessor;
import io.deephaven.test.types.ParallelTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.IntToLongFunction;

@Category(ParallelTest.class)
public class ObjectRegionBinarySearchKernelTest {
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
            // Fetch a random character directly from the static array
            buffer[i] = ALPHA_NUMERIC_CHARS[random.nextInt(ALPHA_NUMERIC_CHARS.length)];
        }

        // A single String allocation at the very end
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

    private void randomizedTestRunner(
            int size, int seed, boolean inverted, IntToLongFunction firstKey, IntToLongFunction lastKey) {

        final Random rnd = new Random(seed);
        final List<String> sortedData = makeSortedData(size, rnd);
        final List<String> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final ColumnRegionObject<String, Values> region = makeColumnRegionObject(data);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        for (int ii = 0; ii < size; ++ii) {
            final String value = data.get(ii);
            final long startRow = Math.max(0, firstKey.applyAsLong(ii));
            final long endRow = Math.min(size - 1, lastKey.applyAsLong(ii));
            // Test match search and min/max search give the same results for this value.
            try (final RowSet matchesFound = ObjectRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new String[] {value});
                    final RowSet minMaxFound = ObjectRegionBinarySearchKernel.binarySearchMinMax(
                            region,
                            startRow, endRow,
                            sortColumn,
                            value,
                            value, true,
                            true);) {
                if (startRow <= ii && ii <= endRow) {
                    Assert.assertTrue("Expected to find " + value + " at index " + ii,
                            matchesFound.containsRange(ii, ii));
                } else {
                    Assert.assertFalse("Index should not be populated.",
                            matchesFound.containsRange(ii, ii));
                }
                Assert.assertEquals("binarySearchMatch and binarySearchMinMax should return the same results.",
                        matchesFound, minMaxFound);
            }

        }

        // Test negative lookups
        final List<String> missingValues =
                findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
        for (String missingValue : missingValues) {
            final long startRow = 0;
            final long endRow = size - 1;
            try (final RowSet valuesFound = ObjectRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new String[] {missingValue})) {
                Assert.assertTrue(valuesFound.isEmpty());
            }
            try (final RowSet valuesFound = ObjectRegionBinarySearchKernel.binarySearchMinMax(
                    region,
                    startRow, endRow,
                    sortColumn,
                    missingValue,
                    missingValue, false,
                    false)) {
                Assert.assertTrue(valuesFound.isEmpty());
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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String maxValue = makeString(rnd, STRING_LENGTH);

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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String minValue = makeString(rnd, STRING_LENGTH);

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
            final List<String> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final String minCandidate = makeString(rnd, STRING_LENGTH);
                final String maxCandidate = makeString(rnd, STRING_LENGTH);

                final String minValue = Collections.min(List.of(minCandidate, maxCandidate));
                final String maxValue = Collections.max(List.of(minCandidate, maxCandidate));

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

    private void maxTestRunner(
            List<String> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
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

        final ColumnRegionObject<String, Values> region = makeColumnRegionObject(dataToUse);

        try (final RowSet result = ObjectRegionBinarySearchKernel.binarySearchMax(
                region, firstKey, lastKey, sortColumn, maxValue, maxInclusive)) {

            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    Assert.assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                Assert.assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final String value = dataToUse.get((int) rowKey);
                if (maxInclusive) {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) <= 0);
                } else {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) < 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                Assert.assertTrue(intersection.isEmpty());
            }
        }
    }

    private void minTestRunner(
            List<String> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
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

        final ColumnRegionObject<String, Values> region = makeColumnRegionObject(dataToUse);

        try (final RowSet result = ObjectRegionBinarySearchKernel.binarySearchMin(
                region, firstKey, lastKey, sortColumn, minValue, minInclusive)) {
            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    Assert.assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                Assert.assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final String value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) >= 0);
                } else {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) > 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                Assert.assertTrue(intersection.isEmpty());
            }
        }
    }

    private void minMaxTestRunner(
            List<String> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
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

        final ColumnRegionObject<String, Values> region = makeColumnRegionObject(dataToUse);

        try (final RowSet result = ObjectRegionBinarySearchKernel.binarySearchMinMax(
                region, firstKey, lastKey, sortColumn, minValue, maxValue, minInclusive, maxInclusive)) {

            // Test from 0 to firstKey - 1 to make sure no false positives are found below the first key.
            if (firstKey > 0) {
                try (final RowSet excludedLow = RowSetFactory.fromRange(0, firstKey - 1);
                        final RowSet intersection = result.intersect(excludedLow)) {
                    Assert.assertTrue(intersection.isEmpty());
                }
            }

            // Go through every value in the result and ensure it is within the min/max bounds.
            result.forAllRowKeys(rowKey -> {
                // Must be within the first/last key bounds
                Assert.assertTrue(rowKey >= firstKey && rowKey <= lastKey);

                // The value at the row key must be within the min/max bounds.
                final String value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) >= 0);
                } else {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, minValue) > 0);
                }
                if (maxInclusive) {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) <= 0);
                } else {
                    Assert.assertTrue(Comparator.<String>naturalOrder().compare(value, maxValue) < 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                    final RowSet intersection = result.intersect(excludedHigh)) {
                Assert.assertTrue(intersection.isEmpty());
            }
        }
    }

    private static final int PAGE_SIZE = 1 << 16;

    private static ColumnRegionObject<String, Values> makeColumnRegionObject(@NotNull final List<String> values) {
        return new AppendOnlyFixedSizePageRegionObject<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, new AppendOnlyRegionAccessor<>() {
                    @Override
                    public void readChunkPage(long firstRowPosition, int minimumSize,
                            @NotNull WritableChunk<Values> destination) {
                        int finalSize = (int) Math.min(minimumSize, values.size() - firstRowPosition);
                        destination.setSize(finalSize);
                        for (int ii = 0; ii < finalSize; ++ii) {
                            destination.asWritableObjectChunk().set(ii, values.get((int) firstRowPosition + ii));
                        }
                    }

                    @Override
                    public long size() {
                        return values.size();
                    }
                });
    }
}

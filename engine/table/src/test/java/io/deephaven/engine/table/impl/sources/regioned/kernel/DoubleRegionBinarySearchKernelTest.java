//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatRegionBinarySearchKernelTest and run "./gradlew replicateRegionAndRegionedSourceTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionDouble;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionDouble;
import io.deephaven.generic.region.AppendOnlyRegionAccessor;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.compare.DoubleComparisons;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntToLongFunction;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

@Category(ParallelTest.class)
public class DoubleRegionBinarySearchKernelTest {
    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static List<Double> makeSortedData(int size, Random rnd) {
        final List<Double> data = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            data.add((double) rnd.nextInt());
        }
        data.sort(DoubleComparisons::compare);
        return data;
    }

    private static List<Double> findAbsentValues(List<Double> sortedData, int num, int maxFailures, Random rnd) {
        final List<Double> missingValues = new ArrayList<>();
        int numFailedLookups = 0;
        while (missingValues.size() < num && numFailedLookups < maxFailures) {
            final double value = (double) rnd.nextInt();
            if (value == NULL_DOUBLE
                    || Collections.binarySearch(sortedData, value, DoubleComparisons::compare) >= 0) {
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
        final List<Double> sortedData = makeSortedData(size, rnd);
        final List<Double> data = new ArrayList<>(sortedData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final ColumnRegionDouble<Values> region = makeColumnRegionDouble(data);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        for (int ii = 0; ii < size; ++ii) {
            final double value = data.get(ii);
            final long startRow = Math.max(0, firstKey.applyAsLong(ii));
            final long endRow = Math.min(size - 1, lastKey.applyAsLong(ii));
            // Test match search and min/max search give the same results for this value.
            try (final RowSet matchesFound = DoubleRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new Double[] {value});
                    final RowSet minMaxFound = DoubleRegionBinarySearchKernel.binarySearchMinMax(
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
        final List<Double> missingValues =
                findAbsentValues(sortedData, NUM_NEGATIVE_LOOKUPS, MAX_FAILED_LOOKUPS, rnd);
        for (Double missingValue : missingValues) {
            final long startRow = 0;
            final long endRow = size - 1;
            try (final RowSet valuesFound = DoubleRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new Double[] {missingValue})) {
                Assert.assertTrue(valuesFound.isEmpty());
            }
            try (final RowSet valuesFound = DoubleRegionBinarySearchKernel.binarySearchMinMax(
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

    /**
     * Match searches for special double values (NULL_DOUBLE, negative infinity, positive infinity, NaN, and -0.0f) should
     * return all rows with that value, and only rows with that value, even NaN (due to the definition of "match")
     */
    @Test
    public void testMatchSpecialDoubles() {
        final List<Double> specialValues = List.of(NULL_DOUBLE, Double.NEGATIVE_INFINITY, (double)-0.0, Double.POSITIVE_INFINITY, Double.NaN);

        // This list will have 2 of each special value.
        final List<Double> sortedData = new ArrayList<>(specialValues);
        sortedData.addAll(specialValues);
        Collections.sort(sortedData, DoubleComparisons::compare);

        final SortColumn sortColumnAsc = SortColumn.asc(ColumnName.of("test"));
        final ColumnRegionDouble<Values> regionAsc = makeColumnRegionDouble(sortedData);

        for (double f : specialValues) {
            try (final RowSet matches = DoubleRegionBinarySearchKernel.binarySearchMatch(
                    regionAsc,
                    0, sortedData.size() - 1,
                    sortColumnAsc,
                    new Double[] {f})) {

                Assert.assertEquals(2, matches.size());
                matches.forAllRowKeys(rowKey -> {
                    final double value = sortedData.get((int) rowKey);
                    Assert.assertTrue(DoubleComparisons.eq(value, f));
                });
            }
        }

        // Repeat the same test for descending order.
        final List<Double> sortedDataDesc = new ArrayList<>(sortedData);
        Collections.reverse(sortedDataDesc);
        final SortColumn sortColumnDesc = SortColumn.desc(ColumnName.of("test"));
        final ColumnRegionDouble<Values> regionDesc = makeColumnRegionDouble(sortedDataDesc);

        for (double f : specialValues) {
            try (final RowSet matches = DoubleRegionBinarySearchKernel.binarySearchMatch(
                    regionDesc,
                    0, sortedData.size() - 1,
                    sortColumnDesc,
                    new Double[] {f})) {

                Assert.assertEquals(2, matches.size());
                matches.forAllRowKeys(rowKey -> {
                    final double value = sortedDataDesc.get((int) rowKey);
                    Assert.assertTrue(DoubleComparisons.eq(value, f));
                });
            }
        }
    }

    /**
     * Range searches for special double values (NULL_DOUBLE, negative infinity, positive infinity, NaN, and -0.0f) should
     * return all rows with that value, and only rows with that value.
     *
     * Note that NaN is not special in this case. When greater than RangeFilter for double are created, the upper
     * bound is set to NaN (exclusive) and the DoubleRegionBinarySearchKernel will honor the exclusive upper bound
     * and exclude NaN values from the results.
     */
    @Test
    public void testMinMaxSpecialDoubles() {
        final List<Double> specialValues = List.of(NULL_DOUBLE, Double.NEGATIVE_INFINITY, (double)-0.0, Double.POSITIVE_INFINITY, Double.NaN);

        // This list will have 2 of each special value.
        final List<Double> sortedData = new ArrayList<>(specialValues);
        sortedData.addAll(specialValues);
        Collections.sort(sortedData, DoubleComparisons::compare);

        final SortColumn sortColumnAsc = SortColumn.asc(ColumnName.of("test"));
        final ColumnRegionDouble<Values> regionAsc = makeColumnRegionDouble(sortedData);

        for (double f : specialValues) {
            try (final RowSet matches = DoubleRegionBinarySearchKernel.binarySearchMinMax(
                    regionAsc,
                    0, sortedData.size() - 1,
                    sortColumnAsc,
                    f, f, true, true)) {

                Assert.assertEquals(2, matches.size());
                matches.forAllRowKeys(rowKey -> {
                    final double value = sortedData.get((int) rowKey);
                    Assert.assertTrue(DoubleComparisons.eq(value, f));
                });
            }
        }

        // Repeat the same test for descending order.
        final List<Double> sortedDataDesc = new ArrayList<>(sortedData);
        Collections.reverse(sortedDataDesc);
        final SortColumn sortColumnDesc = SortColumn.desc(ColumnName.of("test"));
        final ColumnRegionDouble<Values> regionDesc = makeColumnRegionDouble(sortedDataDesc);

        for (double f : specialValues) {
            try (final RowSet matches = DoubleRegionBinarySearchKernel.binarySearchMinMax(
                    regionDesc,
                    0, sortedDataDesc.size() - 1,
                    sortColumnDesc,
                    f, f, true, true)) {

                Assert.assertEquals(2, matches.size());
                matches.forAllRowKeys(rowKey -> {
                    final double value = sortedDataDesc.get((int) rowKey);
                    Assert.assertTrue(DoubleComparisons.eq(value, f));
                });
            }
        }
    }

    @Test
    public void testBinSearchMaxRandom() {
        final Random rnd = new Random(0);

        final int steps = 20;
        for (int size : SIZES) {
            final List<Double> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final double maxValue = (double) rnd.nextInt();

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
            final List<Double> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final double minValue = (double) rnd.nextInt();

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
            final List<Double> data = makeSortedData(size, rnd);

            for (int step = 0; step < steps; ++step) {
                System.out.println("Size = " + size + ", step = " + step);
                final double minCandidate = (double) rnd.nextInt();
                final double maxCandidate = (double) rnd.nextInt();

                final double minValue = (double) Math.min(minCandidate, maxCandidate);
                final double maxValue = (double) Math.max(minCandidate, maxCandidate);

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

    private void minMaxTestRunner(
            List<Double> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final double minValue,
            final boolean minInclusive,
            final double maxValue,
            final boolean maxInclusive) {

        final List<Double> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionDouble<Values> region = makeColumnRegionDouble(dataToUse);

        try (final RowSet result = DoubleRegionBinarySearchKernel.binarySearchMinMax(
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
                final double value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    Assert.assertTrue(DoubleComparisons.compare(value, minValue) >= 0);
                } else {
                    Assert.assertTrue(DoubleComparisons.compare(value, minValue) > 0);
                }
                if (maxInclusive) {
                    Assert.assertTrue(DoubleComparisons.compare(value, maxValue) <= 0);
                } else {
                    Assert.assertTrue(DoubleComparisons.compare(value, maxValue) < 0);
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
            List<Double> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final double minValue,
            final boolean minInclusive) {

        final List<Double> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionDouble<Values> region = makeColumnRegionDouble(dataToUse);

        try (final RowSet result = DoubleRegionBinarySearchKernel.binarySearchMin(
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
                final double value = dataToUse.get((int) rowKey);
                if (minInclusive) {
                    Assert.assertTrue(DoubleComparisons.compare(value, minValue) >= 0);
                } else {
                    Assert.assertTrue(DoubleComparisons.compare(value, minValue) > 0);
                }
            });

            // Test from lastKey + 1 to make sure no false positives are found above the lastKey.
            try (final RowSet excludedHigh = RowSetFactory.fromRange(lastKey + 1, Long.MAX_VALUE);
                 final RowSet intersection = result.intersect(excludedHigh)) {
                Assert.assertTrue(intersection.isEmpty());
            }
        }
    }

    private void maxTestRunner(
            List<Double> data,
            final boolean inverted,
            final long firstKey,
            final long lastKey,
            final double maxValue,
            final boolean maxInclusive) {

        final List<Double> dataToUse;
        final SortColumn sortColumn;
        if (inverted) {
            dataToUse = new ArrayList<>(data);
            Collections.reverse(dataToUse);
            sortColumn = SortColumn.desc(ColumnName.of("test"));
        } else {
            dataToUse = data;
            sortColumn = SortColumn.asc(ColumnName.of("test"));
        }

        final ColumnRegionDouble<Values> region = makeColumnRegionDouble(dataToUse);

        try (final RowSet result = DoubleRegionBinarySearchKernel.binarySearchMax(
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
                final double value = dataToUse.get((int) rowKey);
                if (maxInclusive) {
                    Assert.assertTrue(DoubleComparisons.compare(value, maxValue) <= 0);
                } else {
                    Assert.assertTrue(DoubleComparisons.compare(value, maxValue) < 0);
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

    private static ColumnRegionDouble<Values> makeColumnRegionDouble(@NotNull final List<Double> values) {
        return new AppendOnlyFixedSizePageRegionDouble<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, new AppendOnlyRegionAccessor<>() {
                    @Override
                    public void readChunkPage(long firstRowPosition, int minimumSize,
                            @NotNull WritableChunk<Values> destination) {
                        int finalSize = (int) Math.min(minimumSize, values.size() - firstRowPosition);
                        destination.setSize(finalSize);
                        for (int ii = 0; ii < finalSize; ++ii) {
                            destination.asWritableDoubleChunk().set(ii, values.get((int) firstRowPosition + ii));
                        }
                    }

                    @Override
                    public long size() {
                        return values.size();
                    }
                });
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionShort;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.generic.region.AppendOnlyFixedSizePageRegionShort;
import io.deephaven.generic.region.AppendOnlyRegionAccessor;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.ShortComparisons;
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

@Category(ParallelTest.class)
public class ShortRegionBinarySearchKernelTest {
    private static final int[] SIZES = {10, 100, 1000000};
    private static final int MAX_FAILED_LOOKUPS = 1000;
    private static final int NUM_NEGATIVE_LOOKUPS = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private void randomizedTestRunner(
            int size, int seed, boolean inverted, IntToLongFunction firstKey, IntToLongFunction lastKey) {

        final Random rnd = new Random(seed);
        final List<Short> origData = new ArrayList<>(size);
        for (int ii = 0; ii < size; ++ii) {
            origData.add((short) rnd.nextInt());
        }
        origData.sort(ShortComparisons::compare);
        final List<Short> data = new ArrayList<>(origData);
        if (inverted) {
            java.util.Collections.reverse(data);
        }
        final ColumnRegionShort<Values> region = makeColumnRegionShort(data);
        ColumnName columnName = ColumnName.of("test");
        final SortColumn sortColumn = inverted ? SortColumn.desc(columnName) : SortColumn.asc(columnName);

        for (int ii = 0; ii < size; ++ii) {
            final short value = data.get(ii);
            final long startRow = Math.max(0, firstKey.applyAsLong(ii));
            final long endRow = Math.min(size - 1, lastKey.applyAsLong(ii));
            try (final RowSet valuesFound = ShortRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new Short[] {value})) {
                if (startRow <= ii && ii <= endRow) {
                    Assert.assertTrue("Expected to find " + value + " at index " + ii,
                            valuesFound.containsRange(ii, ii));
                } else {
                    Assert.assertFalse("Index should not be populated.",
                            valuesFound.containsRange(ii, ii));
                }
            }
        }

        // Test negative lookups
        int numFailedLookups = 0;
        for (int ii = 0; ii < NUM_NEGATIVE_LOOKUPS && numFailedLookups < MAX_FAILED_LOOKUPS; ++ii) {
            final short value = (short) rnd.nextInt();
            if (value == QueryConstants.NULL_SHORT
                    || Collections.binarySearch(origData, value, ShortComparisons::compare) >= 0) {
                --ii;
                ++numFailedLookups;
                continue;
            }

            final long startRow = 0;
            final long endRow = size - 1;
            try (final RowSet valuesFound = ShortRegionBinarySearchKernel.binarySearchMatch(
                    region,
                    startRow, endRow,
                    sortColumn,
                    new Short[] {value})) {
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

    private static final int PAGE_SIZE = 1 << 16;

    private static ColumnRegionShort<Values> makeColumnRegionShort(@NotNull final List<Short> values) {
        return new AppendOnlyFixedSizePageRegionShort<>(
                RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK, PAGE_SIZE, new AppendOnlyRegionAccessor<>() {
                    @Override
                    public void readChunkPage(long firstRowPosition, int minimumSize,
                            @NotNull WritableChunk<Values> destination) {
                        int finalSize = (int) Math.min(minimumSize, values.size() - firstRowPosition);
                        destination.setSize(finalSize);
                        for (int ii = 0; ii < finalSize; ++ii) {
                            destination.asWritableShortChunk().set(ii, values.get((int) firstRowPosition + ii));
                        }
                    }

                    @Override
                    public long size() {
                        return values.size();
                    }
                });
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.ParallelTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the region binary search kernels operate correctly on row keys beyond the range of a 32-bit int. This
 * is not covered by the replicated per-type tests, which materialize their data and so stay well below
 * {@link Integer#MAX_VALUE}.
 *
 * <p>
 * Only the char kernel is exercised: all of the primitive kernels are replicated from the same source, so the row key
 * arithmetic under test is identical across types.
 */
@Category(ParallelTest.class)
public class RegionBinarySearchKernelLargeRowKeyTest {
    /** The first row key holding {@link #HIGH_VALUE}; chosen to sit just past {@link Integer#MAX_VALUE}. */
    private static final long SPLIT = Integer.MAX_VALUE + 1L;
    /** Row keys are searched over a small window straddling {@link #SPLIT}. */
    private static final long WINDOW = 4;

    private static final char LOW_VALUE = 'a';
    private static final char HIGH_VALUE = 'b';

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * An ascending-sorted region that computes its values from the row key, so that arbitrarily high row keys can be
     * searched without materializing any data. Row keys below {@link #SPLIT} hold {@link #LOW_VALUE}; the rest hold
     * {@link #HIGH_VALUE}.
     */
    private static final class SplitValueRegion extends GenericColumnRegionBase<Values>
            implements ColumnRegionChar<Values> {

        private SplitValueRegion() {
            super(RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK);
        }

        @Override
        public char getChar(final long elementIndex) {
            assertTrue("Row key " + elementIndex + " is not a valid position", elementIndex >= 0);
            return elementIndex < SPLIT ? LOW_VALUE : HIGH_VALUE;
        }

        @Override
        public void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            throw new UnsupportedOperationException("Binary search should not fill chunks");
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence.Iterator rowSequenceIterator) {
            throw new UnsupportedOperationException("Binary search should not fill chunks");
        }
    }

    private static final SortColumn ASCENDING = SortColumn.asc(ColumnName.of("test"));

    private static final long FIRST_KEY = SPLIT - WINDOW;
    private static final long LAST_KEY = SPLIT + WINDOW;

    @Test
    public void testBinarySearchMatchAboveIntRange() {
        final ColumnRegionChar<Values> region = new SplitValueRegion();
        try (final RowSet result = CharRegionBinarySearchKernel.binarySearchMatch(
                region, FIRST_KEY, LAST_KEY, ASCENDING, new Character[] {HIGH_VALUE})) {
            assertEquals(WINDOW + 1, result.size());
            assertEquals(SPLIT, result.firstRowKey());
            assertEquals(LAST_KEY, result.lastRowKey());
        }
    }

    @Test
    public void testBinarySearchMinAboveIntRange() {
        final ColumnRegionChar<Values> region = new SplitValueRegion();
        try (final RowSet result = CharRegionBinarySearchKernel.binarySearchMin(
                region, FIRST_KEY, LAST_KEY, ASCENDING, HIGH_VALUE, true)) {
            assertEquals(WINDOW + 1, result.size());
            assertEquals(SPLIT, result.firstRowKey());
            assertEquals(LAST_KEY, result.lastRowKey());
        }
    }

    @Test
    public void testBinarySearchMaxAboveIntRange() {
        final ColumnRegionChar<Values> region = new SplitValueRegion();
        try (final RowSet result = CharRegionBinarySearchKernel.binarySearchMax(
                region, FIRST_KEY, LAST_KEY, ASCENDING, LOW_VALUE, true)) {
            assertEquals(WINDOW, result.size());
            assertEquals(FIRST_KEY, result.firstRowKey());
            assertEquals(SPLIT - 1, result.lastRowKey());
        }
    }

    @Test
    public void testBinarySearchMinMaxAboveIntRange() {
        final ColumnRegionChar<Values> region = new SplitValueRegion();
        try (final RowSet result = CharRegionBinarySearchKernel.binarySearchMinMax(
                region, FIRST_KEY, LAST_KEY, ASCENDING, HIGH_VALUE, HIGH_VALUE, true, true)) {
            assertEquals(WINDOW + 1, result.size());
            assertEquals(SPLIT, result.firstRowKey());
            assertEquals(LAST_KEY, result.lastRowKey());
        }
    }

    @Test
    public void testBinarySearchMinMaxSpanningSplitAboveIntRange() {
        final ColumnRegionChar<Values> region = new SplitValueRegion();
        try (final RowSet result = CharRegionBinarySearchKernel.binarySearchMinMax(
                region, FIRST_KEY, LAST_KEY, ASCENDING, LOW_VALUE, HIGH_VALUE, true, true)) {
            assertEquals(2 * WINDOW + 1, result.size());
            assertEquals(FIRST_KEY, result.firstRowKey());
            assertEquals(LAST_KEY, result.lastRowKey());
        }
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ObjectChunkColumnSource;
import io.deephaven.test.types.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Coverage for the column match search over a type whose natural ordering is inconsistent with equals, which is the
 * case {@link ComparableColumnBinarySearchKernel} exists to serve. {@link ObjectColumnBinarySearchKernelTest} covers
 * the ordering-consistent types that take the fast path instead.
 */
@Category(ParallelTest.class)
public class ComparableColumnBinarySearchKernelTest {

    /**
     * The search navigates by {@code ObjectComparisons.compare} but a match is decided by {@code ObjectComparisons.eq},
     * and for a type whose natural ordering is inconsistent with equals those disagree:
     * {@code new BigDecimal("1.0").compareTo(new BigDecimal("1.00")) == 0} while the two are not equal. Ordering-equal
     * values therefore share a contiguous run, and only some members of that run may match. Exactly the equal ones must
     * be returned -- the relation the chunk filter this feeds uses -- whichever member the search happens to land on.
     *
     * <p>
     * Row order within the run is significant, since a sort leaves ordering-equal values in their original order, so
     * each arrangement is driven explicitly rather than left to a fixture.
     */
    @Test
    public void testMatchAcrossOrderingEqualRun() {
        final BigDecimal oneScale1 = new BigDecimal("1.0");
        final BigDecimal oneScale2 = new BigDecimal("1.00");
        final BigDecimal oneScale0 = new BigDecimal("1");
        final BigDecimal two = new BigDecimal("2.0");

        // Every arrangement of the ordering-equal run, so the outcome cannot depend on where the search lands.
        assertMatch(List.of(oneScale1, oneScale2, two), oneScale1, List.of(0L));
        assertMatch(List.of(oneScale2, oneScale1, two), oneScale1, List.of(1L));
        assertMatch(List.of(oneScale2, oneScale0, oneScale1, two), oneScale1, List.of(2L));
        assertMatch(List.of(oneScale1, oneScale2, oneScale0, two), oneScale1, List.of(0L));
        assertMatch(List.of(oneScale0, oneScale1, oneScale2, two), oneScale2, List.of(2L));
        assertMatch(List.of(oneScale0, oneScale1, oneScale2, two), oneScale0, List.of(0L));

        // A value that is only ordering-equal to the run, never equal to any member, matches nothing.
        assertMatch(List.of(oneScale1, oneScale2, two), new BigDecimal("1.000"), List.of());

        // Duplicates of the same representation are all returned.
        assertMatch(List.of(oneScale2, oneScale1, oneScale1, two), oneScale1, List.of(1L, 2L));

        // And the ordinary case is unaffected.
        assertMatch(List.of(oneScale1, oneScale2, two), two, List.of(3L - 1L));
    }

    /**
     * A group of search values that are ordering-equal to each other -- and so to the same run -- share that run, and
     * the run is a superset of the matches for the whole group. Every member must therefore be given the chance to
     * claim its rows before the search moves past the run, whichever member the search navigates by and whether or not
     * that member matches anything itself.
     */
    @Test
    public void testMultiValueMatchAcrossOrderingEqualRun() {
        final BigDecimal oneScale1 = new BigDecimal("1.0");
        final BigDecimal oneScale2 = new BigDecimal("1.00");
        final BigDecimal oneScale0 = new BigDecimal("1");
        final BigDecimal oneScale3 = new BigDecimal("1.000");
        final BigDecimal two = new BigDecimal("2.0");

        // Two members of one run, each named by a search value: both rows match.
        assertMatch(List.of(oneScale1, oneScale2, two), List.of(oneScale1, oneScale2), List.of(0L, 1L));
        // The same, with the run at the end of the data, so the loop's forward progress leaves nothing to search.
        assertMatch(List.of(oneScale1, oneScale2), List.of(oneScale1, oneScale2), List.of(0L, 1L));
        // Three members of one run, one search value not present.
        assertMatch(List.of(oneScale1, oneScale2, oneScale0, two), List.of(oneScale2, oneScale0, oneScale3),
                List.of(1L, 2L));

        // A search value absent from the run must not consume it, in either order within the group.
        assertMatch(List.of(oneScale1, two), List.of(oneScale3, oneScale1), List.of(0L));
        assertMatch(List.of(oneScale1, two), List.of(oneScale1, oneScale3), List.of(0L));

        // A group whose every member is absent from the run matches nothing.
        assertMatch(List.of(oneScale1, oneScale2, two), List.of(oneScale3, new BigDecimal("1.0000")), List.of());

        // Duplicates within a group return each matching row once, not once per member.
        assertMatch(List.of(oneScale1, oneScale2, two), List.of(oneScale1, oneScale1), List.of(0L));

        // A later value beyond the run is still found, so consuming the run as a group keeps the search advancing.
        assertMatch(List.of(oneScale1, oneScale2, two), List.of(oneScale1, oneScale2, two), List.of(0L, 1L, 2L));
        assertMatch(List.of(oneScale1, oneScale2, two), List.of(oneScale3, two), List.of(2L));
    }

    /** Asserts a single-value search, per {@link #assertMatch(List, List, List)}. */
    private static void assertMatch(
            final List<BigDecimal> ascending,
            final BigDecimal toFind,
            final List<Long> expectedPositions) {
        assertMatch(ascending, List.of(toFind), expectedPositions);
    }

    /**
     * Asserts that searching {@code ascending} for {@code toFind} returns exactly {@code expectedPositions}, checked in
     * both sort directions; for descending the data and expected positions are mirrored.
     */
    private static void assertMatch(
            final List<BigDecimal> ascending,
            final List<BigDecimal> toFind,
            final List<Long> expectedPositions) {
        for (final boolean descending : new boolean[] {false, true}) {
            final List<BigDecimal> data;
            final List<Long> expected = new ArrayList<>();
            if (descending) {
                data = new ArrayList<>(ascending);
                Collections.reverse(data);
                for (int ii = expectedPositions.size() - 1; ii >= 0; --ii) {
                    expected.add(ascending.size() - 1 - expectedPositions.get(ii));
                }
            } else {
                data = ascending;
                expected.addAll(expectedPositions);
            }
            @SuppressWarnings("unchecked")
            final ObjectChunkColumnSource<BigDecimal> source =
                    (ObjectChunkColumnSource<BigDecimal>) ChunkColumnSource.make(ChunkType.Object, BigDecimal.class);
            source.addChunk(ObjectChunk.chunkWrap(data.toArray(new BigDecimal[0])));
            final SortColumn sortColumn = descending
                    ? SortColumn.desc(ColumnName.of("test"))
                    : SortColumn.asc(ColumnName.of("test"));
            try (final RowSet selection = RowSetFactory.fromRange(0, data.size() - 1);
                    final RowSet matched = ComparableColumnBinarySearchKernel.binarySearchMatch(
                            source, selection, sortColumn, toFind.toArray(), false)) {
                final List<Long> actual = new ArrayList<>();
                matched.forAllRowKeys(actual::add);
                assertEquals("descending=" + descending + " data=" + data + " toFind=" + toFind,
                        expected, actual);
            }
        }
    }
}

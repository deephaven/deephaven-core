//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.IntPredicate;

import static io.deephaven.api.agg.Aggregation.AggCountWhere;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static org.junit.Assert.assertEquals;

/**
 * {@link ChunkFilter#filterAnd} returns the number of values that are {@code true} in the results after the filter is
 * applied, for every implementation.
 */
public class ChunkFilterFilterAndTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int SIZE = 10;

    /** Seed every third result false, so that the filter must leave those alone. */
    private static WritableBooleanChunk<Values> seededResults() {
        final WritableBooleanChunk<Values> results = WritableBooleanChunk.makeWritableChunk(SIZE);
        for (int ii = 0; ii < SIZE; ++ii) {
            results.set(ii, ii % 3 != 0);
        }
        return results;
    }

    private static void checkFilterAnd(
            final ChunkFilter filter,
            final Chunk<? extends Values> values,
            final IntPredicate expectedMatch) {
        try (final WritableBooleanChunk<Values> results = seededResults()) {
            int expectedTrue = 0;
            final boolean[] expected = new boolean[SIZE];
            for (int ii = 0; ii < SIZE; ++ii) {
                expected[ii] = results.get(ii) && expectedMatch.test(ii);
                expectedTrue += expected[ii] ? 1 : 0;
            }
            assertEquals(expectedTrue, filter.filterAnd(values, results));
            for (int ii = 0; ii < SIZE; ++ii) {
                assertEquals("position " + ii, expected[ii], results.get(ii));
            }
        }
    }

    @Test
    public void constantFilters() {
        try (final WritableIntChunk<Values> values = WritableIntChunk.makeWritableChunk(SIZE)) {
            checkFilterAnd(ChunkFilter.TRUE_FILTER_INSTANCE, values, ii -> true);
            checkFilterAnd(ChunkFilter.FALSE_FILTER_INSTANCE, values, ii -> false);
        }
    }

    @Test
    public void typedFilters() {
        try (final WritableIntChunk<Values> ints = WritableIntChunk.makeWritableChunk(SIZE);
                final WritableObjectChunk<String, Values> strings = WritableObjectChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                ints.set(ii, ii);
                strings.set(ii, Integer.toString(ii));
            }
            checkFilterAnd(ChunkMatchFilterFactory.getChunkFilter(int.class, MatchOptions.REGULAR, 1, 2, 3),
                    ints, ii -> ii >= 1 && ii <= 3);
            checkFilterAnd(ChunkMatchFilterFactory.getChunkFilter(String.class, MatchOptions.INVERTED, "4"),
                    strings, ii -> ii != 4);
        }
    }

    /**
     * An empty-valued match filter is a constant filter; as a later filter in a count where, its {@code filterAnd}
     * return value is the count.
     */
    @Test
    public void countWhereWithConstantLaterFilter() {
        final Table source =
                newTable(intCol("X", 1, 2, -3, 4), intCol("Y", 1, 2, 3, 4), stringCol("K", "a", "a", "b", "b"));

        final MatchFilter inNothing = new MatchFilter(MatchOptions.REGULAR, "Y");
        inNothing.init(source.getDefinition());
        final Table noneMatch =
                source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), inNothing)), "K");
        assertTableEquals(newTable(stringCol("K", "a", "b"), longCol("N", 0, 0)), noneMatch);

        final MatchFilter notInNothing = new MatchFilter(MatchOptions.INVERTED, "Y");
        notInNothing.init(source.getDefinition());
        final Table allMatch =
                source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), notInNothing)), "K");
        assertTableEquals(newTable(stringCol("K", "a", "b"), longCol("N", 2, 1)), allMatch);
    }

    /**
     * Without group-by columns, count where takes its count from the return value of the last filter rather than from
     * the results chunk, so a wrong {@code filterAnd} count is visible only here.
     */
    @Test
    public void ungroupedCountWhereWithConstantLaterFilter() {
        final Table source = newTable(intCol("X", 1, 2, -3, 4), intCol("Y", 1, 2, 3, 4));

        final MatchFilter inNothing = new MatchFilter(MatchOptions.REGULAR, "Y");
        inNothing.init(source.getDefinition());
        assertTableEquals(newTable(longCol("N", 0)),
                source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), inNothing))));

        final MatchFilter notInNothing = new MatchFilter(MatchOptions.INVERTED, "Y");
        notInNothing.init(source.getDefinition());
        assertTableEquals(newTable(longCol("N", 3)),
                source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), notInNothing))));
    }

    @Test
    public void ungroupedCountWhereWithConditionLaterFilter() {
        final Table source = newTable(intCol("X", 1, 2, -3, 4), intCol("Y", 1, 2, 3, 4));
        assertTableEquals(newTable(longCol("N", 2)),
                source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), RawString.of("Y % 2 == 0")))));
    }

    /**
     * On a refreshing source, the ungrouped count where adds and removes the {@code filterAnd} counts of the current
     * and previous values, so a modify needs both to be right.
     */
    @Test
    public void ungroupedCountWhereWithConstantLaterFilterRefreshing() {
        final QueryTable source = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                intCol("X", 1, 2, -3, 4), intCol("Y", 1, 2, 3, 4));
        final MatchFilter notInNothing = new MatchFilter(MatchOptions.INVERTED, "Y");
        notInNothing.init(source.getDefinition());
        final Table counted = source.aggBy(AggCountWhere("N", Filter.and(RawString.of("X > 0"), notInNothing)));
        assertTableEquals(newTable(longCol("N", 3)), counted);

        final ControlledUpdateGraph cug = source.getUpdateGraph().cast();

        cug.runWithinUnitTestCycle(() -> {
            addToTable(source, i(4, 5), intCol("X", 5, -6), intCol("Y", 5, 6));
            source.notifyListeners(i(4, 5), i(), i());
        });
        assertTableEquals(newTable(longCol("N", 4)), counted);

        cug.runWithinUnitTestCycle(() -> {
            addToTable(source, i(0, 2), intCol("X", -1, 3), intCol("Y", 1, 3));
            source.notifyListeners(new TableUpdateImpl(i(), i(), i(0, 2), RowSetShiftData.EMPTY,
                    source.newModifiedColumnSet("X")));
        });
        assertTableEquals(newTable(longCol("N", 4)), counted);

        cug.runWithinUnitTestCycle(() -> {
            addToTable(source, i(5), intCol("X", 6), intCol("Y", 6));
            source.notifyListeners(new TableUpdateImpl(i(), i(), i(5), RowSetShiftData.EMPTY,
                    source.newModifiedColumnSet("X")));
        });
        assertTableEquals(newTable(longCol("N", 5)), counted);

        cug.runWithinUnitTestCycle(() -> {
            removeRows(source, i(1, 3));
            source.notifyListeners(i(), i(1, 3), i());
        });
        assertTableEquals(newTable(longCol("N", 3)), counted);
    }
}

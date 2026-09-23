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
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.IntPredicate;

import static io.deephaven.api.agg.Aggregation.AggCountWhere;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
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
}

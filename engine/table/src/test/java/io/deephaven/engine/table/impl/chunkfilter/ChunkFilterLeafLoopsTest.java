//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntPredicate;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.junit.Assert.assertEquals;

/**
 * The range comparators and the one, two, and three value match filters each carry their own copy of the
 * {@code *ChunkFilter} loops. Every such filter must declare its own {@code filter} and {@code filterAnd}, and those
 * loops must agree with the filter's {@code matches} on every value; the set-based filters keep the shared loops.
 */
public class ChunkFilterLeafLoopsTest {

    private static final int SIZE = 1000;
    private static final MatchOptions NAN_MATCH = MatchOptions.builder().nanMatch(true).build();
    private static final MatchOptions NAN_MATCH_INVERTED = MatchOptions.builder().nanMatch(true).inverted(true).build();

    /** Values in [0, 8), with about one in sixteen null. */
    private static int[] randomValues(final long seed) {
        final Random random = new Random(seed);
        final int[] values = new int[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            values[ii] = random.nextInt(16) == 0 ? -1 : random.nextInt(8);
        }
        return values;
    }

    /** Values in [0, 8), with about one in sixteen each of null, NaN and -0.0. */
    private static double[] randomFloatingValues(final long seed) {
        final Random random = new Random(seed);
        final double[] values = new double[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            switch (random.nextInt(16)) {
                case 0:
                    values[ii] = NULL_DOUBLE;
                    break;
                case 1:
                    values[ii] = Double.NaN;
                    break;
                case 2:
                    values[ii] = -0.0;
                    break;
                default:
                    values[ii] = random.nextInt(8);
            }
        }
        return values;
    }

    @Test
    public void charFilters() {
        final int[] raw = randomValues(0);
        try (final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] < 0 ? NULL_CHAR : (char) ('a' + raw[ii]));
            }
            final List<CharChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(CharRangeComparator.makeCharFilter('b', 'e', lowerInclusive, upperInclusive));
                }
            }
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                own.add(CharChunkMatchFilterFactory.makeFilter(options, 'b'));
                own.add(CharChunkMatchFilterFactory.makeFilter(options, 'b', NULL_CHAR));
                own.add(CharChunkMatchFilterFactory.makeFilter(options, 'b', 'd', 'f'));
                checkSharedLoops(CharChunkFilter.class,
                        CharChunkMatchFilterFactory.makeFilter(options, 'b', 'd', 'f', 'h'), values,
                        f -> ii -> ((CharChunkFilter) f).matches(values.get(ii)));
            }
            for (final CharChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void byteFilters() {
        final int[] raw = randomValues(1);
        try (final WritableByteChunk<Values> values = WritableByteChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] < 0 ? NULL_BYTE : (byte) raw[ii]);
            }
            final List<ByteChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(ByteRangeComparator.makeByteFilter((byte) 1, (byte) 4, lowerInclusive, upperInclusive));
                }
            }
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                own.add(ByteChunkMatchFilterFactory.makeFilter(options, (byte) 1));
                own.add(ByteChunkMatchFilterFactory.makeFilter(options, (byte) 1, NULL_BYTE));
                own.add(ByteChunkMatchFilterFactory.makeFilter(options, (byte) 1, (byte) 3, (byte) 5));
                checkSharedLoops(ByteChunkFilter.class,
                        ByteChunkMatchFilterFactory.makeFilter(options, (byte) 1, (byte) 3, (byte) 5, (byte) 7),
                        values, f -> ii -> ((ByteChunkFilter) f).matches(values.get(ii)));
            }
            for (final ByteChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void shortFilters() {
        final int[] raw = randomValues(2);
        try (final WritableShortChunk<Values> values = WritableShortChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] < 0 ? NULL_SHORT : (short) raw[ii]);
            }
            final List<ShortChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(ShortRangeComparator.makeShortFilter((short) 1, (short) 4, lowerInclusive,
                            upperInclusive));
                }
            }
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                own.add(ShortChunkMatchFilterFactory.makeFilter(options, (short) 1));
                own.add(ShortChunkMatchFilterFactory.makeFilter(options, (short) 1, NULL_SHORT));
                own.add(ShortChunkMatchFilterFactory.makeFilter(options, (short) 1, (short) 3, (short) 5));
                checkSharedLoops(ShortChunkFilter.class,
                        ShortChunkMatchFilterFactory.makeFilter(options, (short) 1, (short) 3, (short) 5, (short) 7),
                        values, f -> ii -> ((ShortChunkFilter) f).matches(values.get(ii)));
            }
            for (final ShortChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void intFilters() {
        final int[] raw = randomValues(3);
        try (final WritableIntChunk<Values> values = WritableIntChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] < 0 ? NULL_INT : raw[ii]);
            }
            final List<IntChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(IntRangeComparator.makeIntFilter(1, 4, lowerInclusive, upperInclusive));
                }
            }
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                own.add(IntChunkMatchFilterFactory.makeFilter(options, 1));
                own.add(IntChunkMatchFilterFactory.makeFilter(options, 1, NULL_INT));
                own.add(IntChunkMatchFilterFactory.makeFilter(options, 1, 3, 5));
                checkSharedLoops(IntChunkFilter.class, IntChunkMatchFilterFactory.makeFilter(options, 1, 3, 5, 7),
                        values, f -> ii -> ((IntChunkFilter) f).matches(values.get(ii)));
            }
            for (final IntChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void longFilters() {
        final int[] raw = randomValues(4);
        try (final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] < 0 ? NULL_LONG : raw[ii]);
            }
            final List<LongChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(LongRangeComparator.makeLongFilter(1, 4, lowerInclusive, upperInclusive));
                }
            }
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                own.add(LongChunkMatchFilterFactory.makeFilter(options, 1));
                own.add(LongChunkMatchFilterFactory.makeFilter(options, 1, NULL_LONG));
                own.add(LongChunkMatchFilterFactory.makeFilter(options, 1, 3, 5));
                checkSharedLoops(LongChunkFilter.class, LongChunkMatchFilterFactory.makeFilter(options, 1, 3, 5, 7),
                        values, f -> ii -> ((LongChunkFilter) f).matches(values.get(ii)));
            }
            for (final LongChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void floatFilters() {
        final double[] raw = randomFloatingValues(5);
        try (final WritableFloatChunk<Values> values = WritableFloatChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii] == NULL_DOUBLE ? NULL_FLOAT : (float) raw[ii]);
            }
            final List<FloatChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(FloatRangeComparator.makeFloatFilter(0, 4, lowerInclusive, upperInclusive));
                }
            }
            // with nanMatch, only a set of search values that holds NaN takes the NaN-aware filters
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED,
                    NAN_MATCH, NAN_MATCH_INVERTED}) {
                final float first = options.nanMatch() ? Float.NaN : 1;
                own.add(FloatChunkMatchFilterFactory.makeFilter(options, first));
                own.add(FloatChunkMatchFilterFactory.makeFilter(options, first, NULL_FLOAT));
                own.add(FloatChunkMatchFilterFactory.makeFilter(options, first, 0.0f, 5));
                checkSharedLoops(FloatChunkFilter.class,
                        FloatChunkMatchFilterFactory.makeFilter(options, first, 0.0f, 5, 7), values,
                        f -> ii -> ((FloatChunkFilter) f).matches(values.get(ii)));
            }
            for (final FloatChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    @Test
    public void doubleFilters() {
        final double[] raw = randomFloatingValues(6);
        try (final WritableDoubleChunk<Values> values = WritableDoubleChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                values.set(ii, raw[ii]);
            }
            final List<DoubleChunkFilter> own = new ArrayList<>();
            for (final boolean lowerInclusive : new boolean[] {false, true}) {
                for (final boolean upperInclusive : new boolean[] {false, true}) {
                    own.add(DoubleRangeComparator.makeDoubleFilter(0, 4, lowerInclusive, upperInclusive));
                }
            }
            // with nanMatch, only a set of search values that holds NaN takes the NaN-aware filters
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED,
                    NAN_MATCH, NAN_MATCH_INVERTED}) {
                final double first = options.nanMatch() ? Double.NaN : 1;
                own.add(DoubleChunkMatchFilterFactory.makeFilter(options, first));
                own.add(DoubleChunkMatchFilterFactory.makeFilter(options, first, NULL_DOUBLE));
                own.add(DoubleChunkMatchFilterFactory.makeFilter(options, first, 0.0, 5));
                checkSharedLoops(DoubleChunkFilter.class,
                        DoubleChunkMatchFilterFactory.makeFilter(options, first, 0.0, 5, 7), values,
                        f -> ii -> ((DoubleChunkFilter) f).matches(values.get(ii)));
            }
            for (final DoubleChunkFilter filter : own) {
                checkOwnLoops(filter, values, ii -> filter.matches(values.get(ii)));
            }
        }
    }

    private interface Matcher {
        IntPredicate forFilter(ChunkFilter filter);
    }

    /** The filter must declare its own loops, and they must agree with {@code expectedMatch}. */
    private static void checkOwnLoops(
            final ChunkFilter filter,
            final Chunk<? extends Values> values,
            final IntPredicate expectedMatch) {
        assertLoopsDeclaredBy(filter.getClass(), filter);
        checkLoops(filter, values, expectedMatch);
    }

    /** The filter must use the shared loops of {@code baseClass}, and they must agree with its {@code matches}. */
    private static void checkSharedLoops(
            final Class<? extends ChunkFilter> baseClass,
            final ChunkFilter filter,
            final Chunk<? extends Values> values,
            final Matcher matcher) {
        assertLoopsDeclaredBy(baseClass, filter);
        checkLoops(filter, values, matcher.forFilter(filter));
    }

    private static void assertLoopsDeclaredBy(final Class<?> expected, final ChunkFilter filter) {
        try {
            final Class<?> filterClass = filter.getClass();
            assertEquals(filterClass.getName(), expected, filterClass
                    .getMethod("filter", Chunk.class, LongChunk.class, WritableLongChunk.class)
                    .getDeclaringClass());
            assertEquals(filterClass.getName(), expected, filterClass
                    .getMethod("filter", Chunk.class, WritableBooleanChunk.class)
                    .getDeclaringClass());
            assertEquals(filterClass.getName(), expected, filterClass
                    .getMethod("filterAnd", Chunk.class, WritableBooleanChunk.class)
                    .getDeclaringClass());
        } catch (final NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private static void checkLoops(
            final ChunkFilter filter,
            final Chunk<? extends Values> values,
            final IntPredicate expectedMatch) {
        final String name = filter.getClass().getName();
        final boolean[] expected = new boolean[SIZE];
        int expectedCount = 0;
        for (int ii = 0; ii < SIZE; ++ii) {
            expected[ii] = expectedMatch.test(ii);
            expectedCount += expected[ii] ? 1 : 0;
        }

        try (final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(SIZE);
                final WritableLongChunk<OrderedRowKeys> keyResults = WritableLongChunk.makeWritableChunk(SIZE);
                final WritableBooleanChunk<Values> results = WritableBooleanChunk.makeWritableChunk(SIZE)) {
            for (int ii = 0; ii < SIZE; ++ii) {
                keys.set(ii, 3L * ii + 7);
            }
            filter.filter(values, keys, keyResults);
            assertEquals(name, expectedCount, keyResults.size());
            int next = 0;
            for (int ii = 0; ii < SIZE; ++ii) {
                if (expected[ii]) {
                    assertEquals(name + " position " + ii, keys.get(ii), keyResults.get(next++));
                }
            }

            // start from the opposite of the expected results, so that every position must be written
            for (int ii = 0; ii < SIZE; ++ii) {
                results.set(ii, !expected[ii]);
            }
            assertEquals(name, expectedCount, filter.filter(values, results));
            for (int ii = 0; ii < SIZE; ++ii) {
                assertEquals(name + " position " + ii, expected[ii], results.get(ii));
            }

            // every third position starts false and must stay false
            int expectedAndCount = 0;
            for (int ii = 0; ii < SIZE; ++ii) {
                results.set(ii, ii % 3 != 0);
                expectedAndCount += ii % 3 != 0 && expected[ii] ? 1 : 0;
            }
            assertEquals(name, expectedAndCount, filter.filterAnd(values, results));
            for (int ii = 0; ii < SIZE; ++ii) {
                assertEquals(name + " position " + ii, ii % 3 != 0 && expected[ii], results.get(ii));
            }
        }
    }
}

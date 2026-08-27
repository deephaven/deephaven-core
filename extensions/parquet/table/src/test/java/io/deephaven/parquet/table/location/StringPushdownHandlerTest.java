//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(OutOfBandTest.class)
public class StringPushdownHandlerTest {

    /** U+FF41 FULLWIDTH LATIN SMALL LETTER A -- UTF-8 {@code EF BD 81}, UTF-16 {@code FF41}. */
    private static final String FULLWIDTH_A = "ａ";
    /** U+1F600 GRINNING FACE -- UTF-8 {@code F0 9F 98 80}, UTF-16 surrogate pair {@code D83D DE00}. */
    private static final String EMOJI = "😀";
    /** A lone high surrogate: not a Unicode scalar value, so it has no UTF-8 encoding at all. */
    private static final String LONE_SURROGATE = "\uD800";

    private static final TableDefinition TABLE_DEFINITION =
            TableDefinition.of(ColumnDefinition.ofString("strCol"));

    private static Statistics<?> stringStats(final String minInc, final String maxInc) {
        return stringStatsFromBytes(
                minInc.getBytes(StandardCharsets.UTF_8),
                maxInc.getBytes(StandardCharsets.UTF_8));
    }

    private static Statistics<?> stringStatsFromBytes(final byte[] minInc, final byte[] maxInc) {
        final PrimitiveType col = Types.required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("strCol");
        return Statistics.getBuilderForReading(col)
                .withMin(minInc)
                .withMax(maxInc)
                .withNumNulls(0L)
                .build();
    }

    private static MatchFilter matchFilter(
            @NotNull final MatchOptions matchOptions,
            @NotNull final Object... values) {
        final MatchFilter filter = new MatchFilter(matchOptions, "strCol", values);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    private static ComparableRangeFilter rangeFilter(
            final Comparable<?> lower, final Comparable<?> upper,
            final boolean lowerInclusive, final boolean upperInclusive) {
        final ComparableRangeFilter filter =
                ComparableRangeFilter.makeForTest("strCol", lower, upper, lowerInclusive, upperInclusive);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    private static SingleSidedComparableRangeFilter greaterThanFilter(
            final Comparable<?> pivot, final boolean inclusive) {
        return singleSidedFilter(pivot, inclusive, true);
    }

    private static SingleSidedComparableRangeFilter singleSidedFilter(
            final Comparable<?> pivot, final boolean inclusive, final boolean isGreaterThan) {
        final SingleSidedComparableRangeFilter filter =
                SingleSidedComparableRangeFilter.makeForTest("strCol", pivot, inclusive, isGreaterThan);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    /**
     * Creates the evaluator for the filter and applies it to one row group's statistics, asserting along the way that
     * this handler actually serves the filter.
     */
    private static boolean evaluate(final WhereFilter filter, final Statistics<?> stats) {
        final StatisticsEvaluator evaluator = StringPushdownHandler.maybeCreateEvaluator(filter);
        assertNotNull("handler should serve " + filter, evaluator);
        return apply(evaluator, stats);
    }

    /**
     * The evaluator is created once per filter and reused for every row group, so it must not depend on any particular
     * statistics object, and creating it must not consume or mutate the filter.
     */
    @Test
    public void preparedEvaluatorIsReusableAcrossRowGroups() {
        final StatisticsEvaluator evaluator =
                StringPushdownHandler.maybeCreateEvaluator(matchFilter(MatchOptions.REGULAR, "ddd", "bbb"));
        assertNotNull(evaluator);

        // Same evaluator, three different row groups, each answered on its own merits.
        assertTrue(apply(evaluator, stringStats("aaa", "zzz")));
        assertFalse(apply(evaluator, stringStats("mmm", "zzz")));
        assertTrue(apply(evaluator, stringStats("ccc", "eee")));

        // And repeating a row group gives the same answer -- the evaluator is not consumed by use.
        assertFalse(apply(evaluator, stringStats("mmm", "zzz")));
        assertTrue(apply(evaluator, stringStats("aaa", "zzz")));
    }

    /**
     * Filters this handler does not serve must return {@code null} from {@code prepare} so the dispatcher falls
     * through. Case-insensitive matches are never pushed down at all -- see
     * {@link StringPushdownHandler#maybeCreateEvaluator}.
     */
    @Test
    public void prepareDeclinesFiltersItDoesNotServe() {
        final MatchFilter icase =
                new MatchFilter(MatchOptions.builder().caseInsensitive(true).build(), "strCol", "abc");
        icase.init(TABLE_DEFINITION);
        assertNull(StringPushdownHandler.maybeCreateEvaluator(icase));
    }

    /**
     * The premise for the two tests below: parquet's byte order and Java's {@link String#compareTo} order disagree for
     * this pair, so statistics read as Java strings are not a bounding interval.
     */
    @Test
    public void utf8AndUtf16OrdersDiverge() {
        assertTrue("UTF-8: fullwidth-a sorts before the emoji", Arrays.compareUnsigned(
                FULLWIDTH_A.getBytes(StandardCharsets.UTF_8), EMOJI.getBytes(StandardCharsets.UTF_8)) < 0);
        assertTrue("UTF-16: the emoji sorts before fullwidth-a", EMOJI.compareTo(FULLWIDTH_A) < 0);
    }

    /**
     * A row group of {@code {emoji, fullwidth-a}} has byte-order min=fullwidth-a, max=emoji. Asked whether it may hold
     * fullwidth-a -- which is its own minimum -- the handler must say yes.
     */
    @Test
    public void matchAgainstSupplementaryPlaneStatistics() {
        final Statistics<?> stats = stringStats(FULLWIDTH_A, EMOJI);

        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, FULLWIDTH_A), stats));
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, EMOJI), stats));

        // A value byte-wise outside the range is still excluded -- equality does not depend on which order is used.
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, "a"), stats));
    }

    /**
     * Parquet permits statistics to be <i>truncated</i> bounds. A writer truncating at byte granularity can leave a
     * dangling multi-byte prefix, which is still a valid byte-order lower bound but decodes to a string containing
     * U+FFFD that sorts <i>above</i> the true minimum in Java order. Comparing bytes avoids the decode entirely.
     */
    @Test
    public void truncatedBoundsRemainBounds() {
        final String trueMin = "日本語"; // CJK, UTF-8 E6 97 A5 | E6 9C AC | E8 AA 9E
        final byte[] truncatedMin = Arrays.copyOf(trueMin.getBytes(StandardCharsets.UTF_8), 4);

        // Premise: the truncation is a byte-order bound, but decoding it breaks that property.
        assertTrue(Arrays.compareUnsigned(truncatedMin, trueMin.getBytes(StandardCharsets.UTF_8)) <= 0);
        final String decoded = new String(truncatedMin, StandardCharsets.UTF_8);
        assertTrue("decodes with a replacement character", decoded.indexOf('�') >= 0);
        assertTrue("and then sorts above the true minimum", decoded.compareTo(trueMin) > 0);

        final Statistics<?> stats =
                stringStatsFromBytes(truncatedMin, "￿".getBytes(StandardCharsets.UTF_8));
        assertTrue("a row group whose truncated lower bound came from the value must not exclude it",
                evaluate(matchFilter(MatchOptions.REGULAR, trueMin), stats));
    }

    @Test
    public void matchFilterScenarios() {
        final Statistics<?> stats = stringStats("alpha", "omega");

        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "beta"), stats));
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "zulu", "delta"), stats));
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, "zulu"), stats));
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, "aaa"), stats));

        // Boundary values are inside the closed interval.
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "alpha"), stats));
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "omega"), stats));

        // Empty list, and a null among the values, are both handled conservatively.
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR), stats));
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "beta", null), stats));
    }

    @Test
    public void invertedMatchFilterScenarios() {
        // Gaps remain inside the statistics range.
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED, "ccc"), stringStats("aaa", "hhh")));

        // A single-valued row group entirely covered by the exclusion list.
        assertFalse(evaluate(
                matchFilter(MatchOptions.INVERTED, "foo"), stringStats("foo", "foo")));

        // Same row group, different exclusion.
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED, "bar"), stringStats("foo", "foo")));

        // Empty exclusion list, and a null among the values.
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED), stringStats("a", "b")));
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED, "a", null), stringStats("a", "b")));

        // Statistics span exactly the two excluded values. Adjacent-gap conservatism: the open interval between them
        // is treated as non-empty, so this stays a "maybe" (a string does exist between "bar" and "baz").
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED, "bar", "baz"), stringStats("bar", "baz")));
        assertTrue(evaluate(
                matchFilter(MatchOptions.INVERTED, "x", "y"), stringStats("x", "y")));
    }

    @Test
    public void rangeFilterScenarios() {
        final Statistics<?> stats = stringStats("ccc", "mmm");

        assertTrue(evaluate(rangeFilter("ddd", "kkk", true, true), stats));
        assertTrue(evaluate(rangeFilter("aaa", "zzz", true, true), stats));
        assertFalse(evaluate(rangeFilter("nnn", "zzz", true, true), stats));
        assertFalse(evaluate(rangeFilter("aaa", "bbb", true, true), stats));

        // Touching at a boundary: inclusive overlaps, exclusive does not.
        assertTrue(evaluate(rangeFilter("aaa", "ccc", true, true), stats));
        assertFalse(evaluate(rangeFilter("aaa", "ccc", true, false), stats));

        // Empty filter range.
        assertFalse(evaluate(rangeFilter("kkk", "kkk", false, false), stats));

        // Bounds supplied the wrong way round are not reordered, and must not be treated as an empty range.
        assertTrue(evaluate(rangeFilter("yyy", "bbb", true, true), stats));

        // (min, min] against statistics starting at min: the half-open interval excludes the only value it could
        // have contained.
        assertFalse(evaluate(rangeFilter("aaa", "aaa", false, true), stringStats("aaa", "bbb")));
    }

    /**
     * Range filters are only evaluated in the byte domain when byte order and Java order provably agree for comparisons
     * against the bound, which requires every code point of the bound to be below U+E000. A bound outside that set must
     * fall back to "maybe".
     */
    @Test
    public void rangeFilterDeclinesWhenOrdersMayDiverge() {
        final Statistics<?> stats = stringStats("ccc", "mmm");

        // A supplementary-plane bound: byte order would say the range is entirely above the statistics, but Java order
        // places the emoji below "ａ" and the handler cannot reason about the data's own characters.
        assertTrue(evaluate(rangeFilter(EMOJI, "￿", true, true), stats));

        // A bound in U+E000..U+FFFF is the other half of the same divergence.
        assertTrue(evaluate(rangeFilter("nnn", FULLWIDTH_A, true, true), stats));

        // CJK is below U+E000, so it stays eligible.
        assertFalse(evaluate(
                rangeFilter("日", "本", true, true), stats));
    }

    @Test
    public void singleSidedRangeFilterScenarios() {
        final Statistics<?> stats = stringStats("ccc", "mmm");

        assertTrue(evaluate(greaterThanFilter("aaa", true), stats));
        assertTrue(evaluate(greaterThanFilter("mmm", true), stats));
        assertFalse(evaluate(greaterThanFilter("mmm", false), stats));
        assertFalse(evaluate(greaterThanFilter("zzz", true), stats));

        // Divergent pivot falls back to "maybe".
        assertTrue(evaluate(greaterThanFilter(EMOJI, true), stats));

        // Ported from SingleSidedComparableRangePushdownHandlerTest, which no longer serves String columns.
        final Statistics<?> alphaOmega = stringStats("alpha", "omega");
        assertTrue(evaluate(greaterThanFilter("beta", true), alphaOmega));
        assertTrue(evaluate(greaterThanFilter("omega", true), alphaOmega));
        assertFalse(evaluate(greaterThanFilter("omega", false), alphaOmega));
        assertFalse(evaluate(greaterThanFilter("zzzz", true), alphaOmega));
        assertTrue(evaluate(greaterThanFilter("aardvark", true), alphaOmega));
    }

    /**
     * Nulls reach these handlers in two unrelated ways, and neither may prune.
     * <p>
     * A null among the <i>filter's</i> values or bounds is declined here, because the statistics describe non-null
     * values only and say nothing about whether the row group holds a null. A null in the <i>data</i> is not this
     * handler's problem at all: {@code ParquetTableLocation.pushdownRowGroupMetadata} consults the filter's null
     * behaviour and keeps the row group unless the null count proves there are none.
     */
    @Test
    public void nullMatchValuesArePushedDown() {
        // Built with numNulls == 0, so the row group is proven free of nulls.
        final Statistics<?> stats = stringStats("ccc", "mmm");

        // A String has no sentinel encoding -- a Deephaven null String comes only from a Parquet null -- so the null
        // is dropped from the values and the null gate in StatisticsEvaluator.maybeMakeForFilter answers for them.

        // `X == null` excludes a row group that holds no nulls.
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, new Object[] {null}), stats));

        // `X in ("zzz", null)` prunes exactly as `X == "zzz"` would.
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, "zzz", null), stats));

        // `X not in ("zzz", null)` prunes as `X != "zzz"` would; here [ccc, mmm] holds values other than "zzz".
        assertTrue(evaluate(matchFilter(MatchOptions.INVERTED, "zzz", null), stats));
    }

    /**
     * Null bounds are a different question from null match values, and are still declined. See
     * {@link #nullMatchValuesArePushedDown} for the half that is not.
     */
    @Test
    public void nullRangeBoundsAreDeclined() {
        final Statistics<?> stats = stringStats("ccc", "mmm");

        // Null bounds on a two-sided range. "nnn".."zzz" alone would otherwise be excluded.
        assertTrue(evaluate(rangeFilter(null, "zzz", true, true), stats));
        assertTrue(evaluate(rangeFilter("nnn", null, true, true), stats));

        // Null pivot on a single-sided range.
        assertTrue(evaluate(greaterThanFilter(null, true), stats));
    }

    /**
     * {@code X < v} and {@code X <= v} arrive as a single-sided filter with {@code isGreaterThan == false} (see
     * {@code RangeFilter.makeComparableRangeFilter}). They match null rows, because Deephaven orders null below every
     * value, and that used to be reason enough to decline them here. The null guard in {@code pushdownRowGroupMetadata}
     * now accounts for those rows centrally, so the bound can be read and the comparison evaluated: some value falls
     * below the pivot only if the smallest one does.
     */
    @Test
    public void lessThanIsEvaluated() {
        final Statistics<?> stats = stringStats("ccc", "mmm");

        // Nothing here is below "aaa", so the row group is excluded.
        assertFalse(evaluate(singleSidedFilter("aaa", true, false), stats));
        assertFalse(evaluate(singleSidedFilter("aaa", false, false), stats));

        // Values below "kkk" do exist.
        assertTrue(evaluate(singleSidedFilter("kkk", true, false), stats));

        // Boundary: (min, ...) exclusive of the minimum itself has nothing below it.
        assertTrue(evaluate(singleSidedFilter("ccc", true, false), stats));
        assertFalse(evaluate(singleSidedFilter("ccc", false, false), stats));

        // The greater-than direction is unchanged.
        assertFalse(evaluate(singleSidedFilter("zzz", true, true), stats));
        assertTrue(evaluate(singleSidedFilter("aaa", true, true), stats));
    }

    /**
     * An unpaired surrogate is not a Unicode scalar value and has no UTF-8 form, so {@code String.getBytes(UTF_8)}
     * substitutes {@code '?'} (0x3F) -- exactly the bytes of the genuine string {@code "?"}. Comparing against those
     * bytes is neither order- nor equality-preserving, so such a filter value cannot be pushed down at all.
     * <p>
     * Deephaven can never read one out of a Parquet column ({@code Binary.toStringUsingUTF8} decodes malformed bytes to
     * U+FFFD, and well-formed UTF-8 cannot encode a surrogate), so this only arises for a hand-built filter. It is
     * still not safe to prune on: the comparison the engine performs is well defined in UTF-16, and it disagrees.
     */
    @Test
    public void unpairedSurrogateValuesAreNotPushedDown() {
        assertEquals("the premise: it encodes to the bytes of \"?\"",
                "?", new String(LONE_SURROGATE.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));

        // `X < "\uD800"` matches "A" -- 0x0041 is below 0xD800 in UTF-16 -- but the pivot encodes to 0x3F, which is
        // *above* "A" in byte order, so pruning on it would drop the row group.
        assertTrue("A".compareTo(LONE_SURROGATE) < 0);
        assertTrue(evaluate(singleSidedFilter(LONE_SURROGATE, false, false), stringStats("A", "A")));

        // Two-sided range with a surrogate at either end.
        assertTrue(evaluate(rangeFilter("A", LONE_SURROGATE, true, true), stringStats("B", "B")));
        assertTrue(evaluate(rangeFilter(LONE_SURROGATE, "zzz", true, true), stringStats("B", "B")));

        // `X not in ("\uD800")` matches every row of a row group holding only "?", since "?" is a different string.
        assertTrue(evaluate(matchFilter(MatchOptions.INVERTED, LONE_SURROGATE), stringStats("?", "?")));

        // A regular match is declined for the same reason, though it could not have gone wrong on its own.
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, LONE_SURROGATE), stringStats("?", "?")));

        // One bad value poisons the whole list; the others cannot be trusted to stand for it.
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, "aaa", LONE_SURROGATE), stringStats("zzz", "zzz")));
    }

    /**
     * The encodability check must not catch valid supplementary characters. A surrogate <i>pair</i> is a scalar value
     * with a perfectly good UTF-8 encoding, so those filters keep pruning exactly as before.
     */
    @Test
    public void pairedSurrogatesStillPruneNormally() {
        // The emoji is U+1F600, stored as the pair D83D DE00, and round-trips through UTF-8 unchanged.
        assertEquals(EMOJI, new String(EMOJI.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));

        // Still served, and still able to exclude: nothing in ["aaa", "bbb"] can equal the emoji.
        assertFalse(evaluate(matchFilter(MatchOptions.REGULAR, EMOJI), stringStats("aaa", "bbb")));
        assertTrue(evaluate(matchFilter(MatchOptions.REGULAR, EMOJI), stringStats(FULLWIDTH_A, EMOJI)));
    }

    /**
     * Applies {@code evaluator} to one row group, deriving whether the group is free of nulls from the statistics as
     * {@link ParquetTableLocation} does for a flat column.
     */
    private static boolean apply(final StatisticsEvaluator evaluator, final Statistics<?> stats) {
        return evaluator.maybeOverlaps(stats);
    }

}

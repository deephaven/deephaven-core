//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Pushdown handler for {@code String} columns.
 * <p>
 * <b>Ordering.</b> Parquet defines these statistics as the extremes under <b>unsigned byte-wise</b> order. From the
 * format's {@code ColumnOrder} definition (see the {@code parquet-format} thrift, mirrored in
 * {@code org.apache.parquet.format.ColumnOrder}): {@code UTF8 - unsigned byte-wise comparison},
 * {@code ENUM - unsigned byte-wise comparison}, and, absent a logical type,
 * {@code BYTE_ARRAY - unsigned byte-wise comparison}. That table applies only when the column declares the type-defined
 * order, which {@link ParquetPushdownUtils#areStatisticsUsable} establishes by requiring
 * {@code columnOrder() == typeDefined()}. parquet-mr computes the extremes accordingly, via
 * {@code UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR} -- unsigned bytes, ties broken by length, which is exactly
 * {@link java.util.Arrays#compareUnsigned(byte[], byte[])}.
 * <p>
 * Deephaven, by contrast, compares strings with {@link String#compareTo}, which is <b>UTF-16 code-unit</b> order. The
 * two disagree: UTF-8 encodes a supplementary code point with a lead byte of {@code F0}..{@code F4}, above the
 * {@code EE}/{@code EF} of U+E000..U+FFFF, but UTF-16 encodes it as a surrogate pair whose first unit (D800..DFFF)
 * falls <i>below</i> E000. Decoding the statistics and comparing the results is therefore unsound: for a row group
 * holding both kinds of character the decoded pair is not merely misplaced but inverted, with {@code min > max}.
 * <p>
 * <b>Encoding.</b> This handler works entirely in the byte domain: statistics are read as raw bytes and filter values
 * are encoded to UTF-8. That encoding is the inverse of the read path, which materializes these columns with
 * {@code Binary.toStringUsingUTF8()} (see {@code StringMaterializer}), so a filter value encodes back to the bytes the
 * column was decoded from. Only the two sides agreeing matters here; the handler never decodes, and so never has to
 * assume the stored bytes are well-formed.
 * <p>
 * Staying in bytes is also what makes <i>truncated</i> bounds safe. The format permits statistics to be shortened
 * bounds rather than values present in the data; such a bound still brackets the data in byte order, but decoding one
 * can turn a partial character into U+FFFD and destroy that property.
 * <p>
 * <b>Nulls.</b> {@link StatisticsEvaluator} describes the two reasons a row can read back as null in Deephaven, and
 * neither is this class's business. A String has no sentinel encoding, so a Deephaven null comes solely from a Parquet
 * null, which {@code min}/{@code max} never describe. A null is dropped from a match filter's values here, and
 * {@code StatisticsEvaluator.maybeMakeForFilter} gates on such rows before any of this runs; these evaluators answer
 * from {@code min}/{@code max} alone and are <b>not</b> correct in isolation for a filter that a null row satisfies.
 * Null <i>bounds</i> on a range filter are a separate question, and are declined.
 * <p>
 * <b>Usage.</b> Call {@link #maybeCreateEvaluator} once per filter, then {@link StatisticsEvaluator#maybeOverlaps} once
 * per row group. Everything that depends only on the filter -- encoding the values, sorting them, and testing the
 * bounds for order divergence -- happens in {@code maybeCreateEvaluator}, so none of it is repeated for every row
 * group.
 */
final class StringPushdownHandler {

    private static final Comparator<byte[]> BYTES = Arrays::compareUnsigned;

    /**
     * The first code point for which UTF-8 and UTF-16 disagree on ordering. See
     * {@link #comparesIdenticallyInBothOrders}.
     */
    private static final int FIRST_DIVERGENT_CODE_POINT = 0xE000;

    /**
     * Creates an evaluator for {@code filter} against row group statistics, or returns {@code null} if this handler
     * does not serve it.
     * <p>
     * <b>Case-insensitive matches are never served, by design.</b> These statistics are extremes under byte order, and
     * case-insensitive equality is not monotonic with respect to it, so {@code [min, max]} containment says nothing
     * about whether a case variant of a filter value is present. The natural repair -- widening each value into the
     * interval spanned by its case variants -- does not work either, because {@link String#equalsIgnoreCase} matches
     * characters far outside ASCII to ASCII ones:
     * <ul>
     * <li>U+212A KELVIN SIGN ({@code E2 84 AA}) equals {@code "k"}</li>
     * <li>U+017F LATIN SMALL LETTER LONG S ({@code C5 BF}) equals {@code "s"}</li>
     * <li>U+0130 and U+0131 ({@code C4 B0}, {@code C4 B1}) equal {@code "i"}</li>
     * </ul>
     * A row group whose entire byte range sits above the ASCII range can therefore still contain a match for a purely
     * ASCII filter value, and an ASCII-only interval would wrongly exclude it. A sound interval would have to span the
     * full case-equivalence class of every character, which is wide enough to rarely exclude anything.
     * <p>
     * The inverted form is worse: excluding a row group requires proving it holds a single distinct value, and
     * statistics that are permitted to be <i>truncated</i> bounds cannot establish that.
     * <p>
     * These filters are not lost, only resolved elsewhere -- the dictionary path applies the real chunk filter to a row
     * group's dictionary, which handles case-insensitivity exactly.
     */
    @Nullable
    static StatisticsEvaluator maybeCreateEvaluator(@NotNull final WhereFilter filter) {
        if (filter instanceof MatchFilter) {
            final MatchFilter matchFilter = (MatchFilter) filter;
            if (matchFilter.getColumnType() != String.class || matchFilter.getMatchOptions().caseInsensitive()) {
                return null;
            }
            return createMatchEvaluator(matchFilter);
        }
        if (filter instanceof ComparableRangeFilter) {
            final ComparableRangeFilter rangeFilter = (ComparableRangeFilter) filter;
            return rangeFilter.getColumnType() == String.class ? createRangeEvaluator(rangeFilter) : null;
        }
        if (filter instanceof SingleSidedComparableRangeFilter) {
            final SingleSidedComparableRangeFilter rangeFilter = (SingleSidedComparableRangeFilter) filter;
            return rangeFilter.getColumnType() == String.class ? createSingleSidedEvaluator(rangeFilter) : null;
        }
        return null;
    }

    /**
     * Equality does not depend on the ordering used, so this needs no restriction on the values: a value whose UTF-8
     * encoding falls outside the byte-order interval is simply not present in the row group.
     */
    private static StatisticsEvaluator createMatchEvaluator(@NotNull final MatchFilter matchFilter) {
        final Object[] values = matchFilter.getValues();
        final boolean invertMatch = matchFilter.getMatchOptions().inverted();
        if (values == null || values.length == 0) {
            // No values to check against
            return invertMatch ? StatisticsEvaluator.ALWAYS_MAYBE : statistics -> false;
        }
        // Nulls are dropped here; the null gate in StatisticsEvaluator answers for them.
        final byte[][] allEncoded = new byte[values.length][];
        int numNonNull = 0;
        for (final Object value : values) {
            if (value instanceof String) {
                allEncoded[numNonNull++] = utf8((String) value);
            } else if (value != null) {
                // Not a String, so it has no encoding to compare against the statistics.
                return StatisticsEvaluator.ALWAYS_MAYBE;
            }
            // A null falls through and is simply dropped.
        }
        if (numNonNull == 0) {
            // Nothing but null was given. `X != null` matches any non-null value, and usable statistics guarantee
            // at least one exists; `X == null` reaches here only for a row group with no Parquet nulls to match.
            return invertMatch
                    ? StatisticsEvaluator.ALWAYS_MAYBE
                    : statistics -> false;
        }
        final byte[][] encoded =
                numNonNull == values.length ? allEncoded : Arrays.copyOf(allEncoded, numNonNull);
        if (!invertMatch) {
            return statistics -> {
                final byte[][] minMax = minMaxBytes(statistics);
                if (minMax == null) {
                    // Statistics could not be processed, so we cannot determine overlaps. Assume that we overlap.
                    return true;
                }
                for (final byte[] value : encoded) {
                    if (BYTES.compare(minMax[0], value) <= 0 && BYTES.compare(minMax[1], value) >= 0) {
                        return true;
                    }
                }
                return false;
            };
        }
        // Sorted once here; maybeMatchesInverse walks the gaps between adjacent values, with the same
        // byte comparator it uses. Non-Strings were rejected above and nulls were removed.
        Arrays.sort(encoded, BYTES);
        return statistics -> {
            final byte[][] minMax = minMaxBytes(statistics);
            return minMax == null || maybeMatchesInverse(minMax[0], minMax[1], encoded);
        };
    }

    private static StatisticsEvaluator createRangeEvaluator(@NotNull final ComparableRangeFilter rangeFilter) {
        final Comparable<?> lower = rangeFilter.getLower();
        final Comparable<?> upper = rangeFilter.getUpper();
        if (!(lower instanceof String) || !(upper instanceof String)) {
            // Not reachable from a parsed comparison: RangeFilter always supplies both bounds. Nor is it clear what
            // a null bound should mean -- Deephaven orders null below every value, so a null lower reads as
            // "unbounded below" while a null upper reads as an empty range, and no filter expresses either. Declined
            // rather than guessed.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        if (!comparesIdenticallyInBothOrders((String) lower) || !comparesIdenticallyInBothOrders((String) upper)) {
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final byte[] lowerBytes = utf8((String) lower);
        final byte[] upperBytes = utf8((String) upper);
        final boolean lowerInclusive = rangeFilter.isLowerInclusive();
        final boolean upperInclusive = rangeFilter.isUpperInclusive();
        return statistics -> {
            final byte[][] minMax = minMaxBytes(statistics);
            return minMax == null || overlapsRange(minMax[0], minMax[1],
                    lowerBytes, lowerInclusive, upperBytes, upperInclusive);
        };
    }

    private static StatisticsEvaluator createSingleSidedEvaluator(
            @NotNull final SingleSidedComparableRangeFilter rangeFilter) {
        if (rangeFilter.isLowerInclusive() != rangeFilter.isUpperInclusive()) {
            throw new IllegalStateException("SingleSidedComparableRangeFilter must have both bounds inclusive or " +
                    "exclusive: " + rangeFilter);
        }
        final Comparable<?> pivot = rangeFilter.getPivot();
        if (!(pivot instanceof String)) {
            // Not reachable from a parsed comparison, and null is not orderable against itself here: Deephaven
            // orders null below every value, so `X < null` is empty and `X > null` is everything, and no parsed
            // filter expresses either. Declined rather than guessed.
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        // `X < v` and `X <= v` accept a null row, since Deephaven orders null under every value; `X > v` cannot.
        // A String has no sentinel encoding, so the null gate in maybeMakeForFilter settles that on its own.
        if (!comparesIdenticallyInBothOrders((String) pivot)) {
            return StatisticsEvaluator.ALWAYS_MAYBE;
        }
        final byte[] pivotBytes = utf8((String) pivot);
        final boolean inclusive = rangeFilter.isLowerInclusive();
        final boolean isGreaterThan = rangeFilter.isGreaterThan();
        return statistics -> {
            final byte[][] minMax = minMaxBytes(statistics);
            if (minMax == null) {
                // Statistics could not be processed, so we assume that we overlap.
                return true;
            }
            if (isGreaterThan) {
                // Some value can exceed the pivot only if the largest one does.
                final int cmp = BYTES.compare(minMax[1], pivotBytes);
                return inclusive ? cmp >= 0 : cmp > 0;
            }
            // ... and fall below it only if the smallest one does.
            final int cmp = BYTES.compare(minMax[0], pivotBytes);
            return inclusive ? cmp <= 0 : cmp < 0;
        };
    }

    /**
     * Whether comparisons <i>against</i> {@code value} give the same answer in unsigned byte order as in
     * {@link String#compareTo} order, whatever the other operand is.
     * <p>
     * Two strings are ordered by their first differing code point, and UTF-8 preserves code-point order, so the orders
     * can only disagree when that code point is supplementary in one operand and in U+E000..U+FFFF in the other. If
     * every code point of {@code value} is below U+E000, neither case can arise at the deciding position: any code
     * point of the other operand that is at least U+E000 -- supplementary or not -- compares greater under both
     * encodings. That makes byte-order comparisons against {@code value} sound for an arbitrary counterparty, which is
     * what lets range filters be evaluated in the byte domain at all.
     * <p>
     * This must be a property of the filter's bound, not of the statistics. {@code min}/{@code max} bound only the
     * endpoints of the byte interval and say nothing about its interior, so a row group whose extremes are plain ASCII
     * can still hold a supplementary code point.
     */
    private static boolean comparesIdenticallyInBothOrders(@NotNull final String value) {
        return value.codePoints().allMatch(cp -> cp < FIRST_DIVERGENT_CODE_POINT);
    }

    private static byte[] utf8(@NotNull final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Reads the row group's byte-order extremes, returning {@code null} if they cannot be used.
     */
    @Nullable
    private static byte[][] minMaxBytes(@NotNull final Statistics<?> statistics) {
        final MutableObject<byte[]> min = new MutableObject<>();
        final MutableObject<byte[]> max = new MutableObject<>();
        if (!MinMaxFromStatistics.getMinMaxForStrings(statistics, min::setValue, max::setValue)) {
            return null;
        }
        if (min.getValue() == null || max.getValue() == null) {
            return null;
        }
        return new byte[][] {min.getValue(), max.getValue()};
    }

    /**
     * Verifies that the {@code [min, max]} range includes any value that is not in the given {@code values} array, by
     * checking whether it overlaps any of the open gaps left by excluding them. {@code values} must be sorted.
     */
    private static boolean maybeMatchesInverse(
            @NotNull final byte[] min,
            @NotNull final byte[] max,
            @NotNull final byte[][] values) {
        if (BYTES.compare(min, values[0]) < 0) {
            return true;
        }
        for (int i = 0; i < values.length - 1; i++) {
            if (overlapsRange(min, max, values[i], false, values[i + 1], false)) {
                return true;
            }
        }
        return BYTES.compare(max, values[values.length - 1]) > 0;
    }

    /**
     * Verifies that the {@code [min, max]} range intersects the range defined by the given lower and upper bounds.
     */
    private static boolean overlapsRange(
            @NotNull final byte[] min, @NotNull final byte[] max,
            @NotNull final byte[] lower, final boolean lowerInclusive,
            @NotNull final byte[] upper, final boolean upperInclusive) {
        final int lowerToUpper = BYTES.compare(lower, upper);
        if ((upperInclusive && lowerInclusive) ? lowerToUpper > 0 : lowerToUpper >= 0) {
            return false; // Empty range, no overlap
        }
        // Following logic assumes (min, max) to be a continuous range and not granular. So (a,b) will be considered
        // as "maybe overlapping" with [a, b] where b follows immediately after a.
        final int minToUpper = BYTES.compare(min, upper);
        final int maxToLower = BYTES.compare(max, lower);
        return (upperInclusive ? minToUpper <= 0 : minToUpper < 0)
                && (lowerInclusive ? maxToLower >= 0 : maxToLower > 0);
    }
}

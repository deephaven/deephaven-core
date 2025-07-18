//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

/**
 * Static helpers for hashCode, equality, and comparison of CharSequences.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class CharSequenceUtils {

    /**
     * Calculate the hash code of a CharSequence as if it were a String.
     * 
     * @param charSequence The CharSequence
     * @return A hash code for the specified CharSequence.
     */
    public static int hashCode(@NotNull final CharSequence charSequence) {
        int hashCode = 0;
        final int length = charSequence.length();
        for (int ci = 0; ci < length; ++ci) {
            hashCode = 31 * hashCode + charSequence.charAt(ci);
        }
        return hashCode;
    }

    /**
     * Calculate the hash code of a CharSequence, consistent with
     * {@link #contentEqualsIgnoreCase(CharSequence, CharSequence)}.
     * 
     * @param charSequence The CharSequence
     * @return A hash code for the specified CharSequence.
     */
    public static int caseInsensitiveHashCode(@NotNull final CharSequence charSequence) {
        int hashCode = 0;
        try (final IntStream codePoints = charSequence.codePoints()) {
            final PrimitiveIterator.OfInt it = codePoints.iterator();
            while (it.hasNext()) {
                hashCode = 31 * hashCode + Character.toLowerCase(Character.toUpperCase(it.nextInt()));
            }
        }
        return hashCode;
    }

    /**
     * Compare two CharSequences for equality, disregarding class.
     * 
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return Whether the supplied CharSequences represent an equal sequence of chars.
     */
    public static boolean contentEquals(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
        if (cs1 == cs2) {
            return true;
        }
        final int cs1Length = cs1.length();
        if (cs1Length != cs2.length()) {
            return false;
        }
        for (int ci = 0; ci < cs1Length; ++ci) {
            if (cs1.charAt(ci) != cs2.charAt(ci)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two CharSequences for equality, disregarding class and allowing for nullity.
     * 
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return Whether the supplied CharSequences represent an equal sequence of chars.
     */
    public static boolean nullSafeContentEquals(@Nullable final CharSequence cs1,
            @Nullable final CharSequence cs2) {
        if (cs1 == null || cs2 == null) {
            return cs1 == cs2;
        }
        return contentEquals(cs1, cs2);
    }

    /**
     * Compare two CharSequences ignoring case differences, with the same Unicode code point semantics as
     * {@link String#equalsIgnoreCase(String)}.
     * 
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return Whether the supplied CharSequences represent an equal sequence of chars, ignoring case differences.
     */
    public static boolean contentEqualsIgnoreCase(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
        if (cs1 == cs2) {
            return true;
        }
        if (cs1.length() != cs2.length()) {
            return false;
        }
        try (
                final IntStream cp1 = cs1.codePoints();
                final IntStream cp2 = cs2.codePoints()) {
            final PrimitiveIterator.OfInt it1 = cp1.iterator();
            final PrimitiveIterator.OfInt it2 = cp2.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                final int c1 = it1.nextInt();
                final int c2 = it2.nextInt();
                if (!equalsIgnoreCase(c1, c2)) {
                    return false;
                }
            }
            if (it1.hasNext() || it2.hasNext()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two char sequences lexicographically, ignoring case differences, with the same Unicode code point
     * semantics as {@link String#compareToIgnoreCase(String)}.
     *
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return The comparison between the CharSequences, disregarding case.
     */
    public static int compareToIgnoreCase(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
        if (cs1 == cs2) {
            return 0;
        }
        // Can't use length given we are doing lexigraphical comparison and codepoints may be of different length than
        // number of characters.
        try (
                final IntStream cp1 = cs1.codePoints();
                final IntStream cp2 = cs2.codePoints()) {
            final PrimitiveIterator.OfInt it1 = cp1.iterator();
            final PrimitiveIterator.OfInt it2 = cp2.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                final int c1 = it1.nextInt();
                final int c2 = it2.nextInt();
                final int cmp = compareToIgnoreCase(c1, c2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            if (it2.hasNext()) {
                return -1;
            } else if (it1.hasNext()) {
                return 1;
            }
        }
        return 0;
    }

    private static boolean equalsIgnoreCase(int codePoint1, int codePoint2) {
        if (codePoint1 == codePoint2) {
            return true;
        }
        final int c1Upper = Character.toUpperCase(codePoint1);
        final int c2Upper = Character.toUpperCase(codePoint2);
        if (c1Upper == c2Upper) {
            return true;
        }
        return Character.toLowerCase(c1Upper) == Character.toLowerCase(c2Upper);
    }

    private static int compareToIgnoreCase(int codePoint1, int codePoint2) {
        if (codePoint1 == codePoint2) {
            return 0;
        }
        final int c1Upper = Character.toUpperCase(codePoint1);
        final int c2Upper = Character.toUpperCase(codePoint2);
        if (c1Upper == c2Upper) {
            return 0;
        }
        return Character.toLowerCase(c1Upper) - Character.toLowerCase(c2Upper);
    }

    /**
     * Compare two CharSequences for case-insensitive equality, disregarding class and allowing for nullity.
     * 
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return Whether the supplied CharSequences represent an equal sequence of chars.
     */
    public static boolean nullSafeContentEqualsIgnoreCase(@Nullable final CharSequence cs1,
            @Nullable final CharSequence cs2) {
        if (cs1 == null || cs2 == null) {
            return cs1 == cs2;
        }
        return contentEqualsIgnoreCase(cs1, cs2);
    }

    /**
     * Test content equality for two CharSequence sub-regions. See String.regionMatches(...).
     * 
     * @param ignoreCase Whether to use a case-insensitive comparison
     * @param cs1 The first CharSequence
     * @param cs1Offset The offset into the first CharSequence
     * @param cs2 The second CharSequence
     * @param cs2Offset The offset into the second CharSequence
     * @param length The number of characters to compare
     * @return Whether the regions match
     */
    public static boolean regionMatches(final boolean ignoreCase,
            final CharSequence cs1,
            final int cs1Offset,
            final CharSequence cs2,
            final int cs2Offset,
            final int length) {
        for (int ci = 0; ci < length; ++ci) {
            char cs1Char = cs1.charAt(cs1Offset + ci);
            char cs2Char = cs2.charAt(cs2Offset + ci);
            if (cs1Char == cs2Char) {
                continue;
            }
            if (ignoreCase) {
                cs1Char = Character.toUpperCase(cs1Char);
                cs2Char = Character.toUpperCase(cs2Char);
                if (cs1Char == cs2Char) {
                    continue;
                }
                // Uncomment this if we start caring about any alphabets (e.g. Georgian) that don't have consistent
                // conversion to uppercase.
                // cs1Char = Character.toLowerCase(cs1Char);
                // cs2Char = Character.toLowerCase(cs2Char);
                // if (cs1Char == cs2Char) {
                // continue;
                // }
            }
            return false;
        }
        return true;
    }

    /**
     * A re-usable case-sensitive Comparator for CharSequences.
     */
    public static final Comparator<CharSequence> CASE_SENSITIVE_COMPARATOR = new CaseSensitiveComparator();

    private static class CaseSensitiveComparator implements Comparator<CharSequence> {

        @Override
        public int compare(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
            final int cs1Length = cs1.length();
            final int cs2Length = cs2.length();
            final int minLength = Math.min(cs1Length, cs2Length);

            for (int ci = 0; ci < minLength; ++ci) {
                final char cs1Char = cs1.charAt(ci);
                final char cs2Char = cs2.charAt(ci);
                if (cs1Char != cs2Char) {
                    return cs1Char - cs2Char;
                }
            }
            return cs1Length - cs2Length;
        }
    }

    /**
     * A re-usable case-insensitive Comparator for CharSequences.
     */
    public static final Comparator<CharSequence> CASE_INSENSITIVE_COMPARATOR = new CaseInsensitiveComparator();

    private static class CaseInsensitiveComparator implements Comparator<CharSequence> {

        @Override
        public int compare(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
            return compareToIgnoreCase(cs1, cs2);
        }
    }
}

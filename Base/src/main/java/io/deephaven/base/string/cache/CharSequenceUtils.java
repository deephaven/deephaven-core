/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

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
     * Calculate the hash code of a CharSequence as if it were an uppercase String.
     * 
     * @param charSequence The CharSequence
     * @return A hash code for the specified CharSequence.
     */
    public static int caseInsensitiveHashCode(@NotNull final CharSequence charSequence) {
        int hashCode = 0;
        final int length = charSequence.length();
        for (int ci = 0; ci < length; ++ci) {
            hashCode = 31 * hashCode + Character.toUpperCase(charSequence.charAt(ci));
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
     * Compare two CharSequences for case-insensitive equality, disregarding class.
     * 
     * @param cs1 The first CharSequence
     * @param cs2 The second CharSequence
     * @return Whether the supplied CharSequences represent an equal sequence of chars, disregarding case.
     */
    public static boolean contentEqualsIgnoreCase(@NotNull final CharSequence cs1, @NotNull final CharSequence cs2) {
        if (cs1 == cs2) {
            return true;
        }
        final int cs1Length = cs1.length();
        if (cs1Length != cs2.length()) {
            return false;
        }
        for (int ci = 0; ci < cs1Length; ++ci) {
            char cs1Char = cs1.charAt(ci);
            char cs2Char = cs2.charAt(ci);
            if (cs1Char == cs2Char) {
                continue;
            }
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
            return false;
        }
        return true;
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
            final int cs1Length = cs1.length();
            final int cs2Length = cs2.length();
            final int minLength = Math.min(cs1Length, cs2Length);

            for (int ci = 0; ci < minLength; ++ci) {
                char cs1Char = cs1.charAt(ci);
                char cs2Char = cs2.charAt(ci);
                if (cs1Char == cs2Char) {
                    continue;
                }
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
                return cs1Char - cs2Char;
            }
            return cs1Length - cs2Length;
        }
    }
}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;

import static io.deephaven.util.QueryConstants.NULL_INT;

public class TestMatchFilterOverlap extends RefreshingTableTestCase {

    /**
     * Assert some fundamental properties of the java lexicographical ordering:
     * <ol>
     * <li>converting a string to upper case *lowers* (or maintains) the ordering (because of the character set
     * ordering)</li>
     * <li>converting a string to lower case *raises* (or maintains) the ordering</li>
     * </ol>
     * This test should never fail, but we leverage these axioms in the MatchFilter range overlap functionality and
     * prefer to be informed if they are violated in the future.
     */
    public void testLexigraphicalOrdering() {
        final String[] inputString = {
                "AAAAA",
                "aaaaa",
                "aAaAa",
                "AaAaA",
                "A1b2C3",
                "a1B2c3",
                "12345",
                "#$%&#"
        };

        for (final String s : inputString) {
            // noinspection EqualsWithItself
            assert s.compareTo(s) == 0;

            // s is <= than lower case s
            assert s.compareTo(s.toLowerCase()) <= 0;

            // s is >= than upper case s
            assert s.compareTo(s.toUpperCase()) >= 0;
        }
    }

    public void testMatchFilterCaseSensitiveString() {
        MatchFilter f;

        // Case-sensitive string filter, no match
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "a", "b");
        assertFalse(f.overlaps("c", "e", false, false));

        // Case-sensitive string filter, no match
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "b", "c");
        assertFalse(f.overlaps("c", "e", false, false));

        // Case-sensitive string filter, no match
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "e", "c");
        assertFalse(f.overlaps("c", "e", false, false));

        // Case-sensitive string filter, match lower
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "b", "c");
        assertTrue(f.overlaps("c", "e", true, false));

        // Case-sensitive string filter, match upper
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "e", "c");
        assertTrue(f.overlaps("c", "e", false, true));

        // Case-sensitive string filter, match middle
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "d", "c");
        assertTrue(f.overlaps("c", "e", false, true));

        // Inverted case-sensitive string filter, no match
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "b", "c");
        assertTrue(f.overlaps("c", "e", false, false));

        // Inverted case-sensitive string filter, no match
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "e", "c");
        assertTrue(f.overlaps("c", "e", false, false));

        // Inverted case-sensitive string filter, match lower
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "b", "c");
        assertFalse(f.overlaps("c", "e", true, false));

        // Inverted case-sensitive string filter, match upper
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "e", "c");
        assertFalse(f.overlaps("c", "e", false, true));

        // Inverted case-sensitive string filter, match middle
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "d", "c");
        assertFalse(f.overlaps("c", "e", false, true));

        // Null testing
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "d", null, "c");
        assertTrue(f.containsNull());

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Inverted,
                "A", "d", null, "c");
        assertFalse(f.containsNull());

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.MatchCase,
                MatchFilter.MatchType.Regular,
                "A", "a", "b");
        assertFalse(f.containsNull());
    }

    public void testMatchFilterCaseInsensitiveString() {
        MatchFilter f;

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "a", "b");
        // NOTE: we bump "c" to "C" which is < "a" for java lexicographic comparison, so this is a match.
        assertTrue(f.overlaps("c", "e", false, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "AAAA");
        // NOTE: "AAAA" is < "BBBB"/"bbbb" so is not contained
        assertFalse(f.overlaps("bbbb", "zzzz", false, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "AAAA");
        // NOTE: lower bound of "aaaa" becomes "AAAA" but not inclusive, so not contained
        assertFalse(f.overlaps("aaaa", "zzzz", false, false));
        // When lowerInclusive is true, is contained
        assertTrue(f.overlaps("aaaa", "zzzz", true, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "zzzz");
        // NOTE: upper bound of "zzzz" unchanged but not inclusive, so not contained
        assertFalse(f.overlaps("aaaa", "zzzz", false, false));
        // When upperInclusive is true, is contained
        assertTrue(f.overlaps("aaaa", "zzzz", false, true));

        // Inverted versions of the above tests

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Inverted,
                "A", "a", "b");
        assertFalse(f.overlaps("c", "e", false, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Inverted,
                "A", "AAAA");
        assertTrue(f.overlaps("bbbb", "zzzz", false, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Inverted,
                "A", "AAAA");
        assertTrue(f.overlaps("aaaa", "zzzz", false, false));
        assertFalse(f.overlaps("aaaa", "zzzz", true, false));

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Inverted,
                "A", "zzzz");
        assertTrue(f.overlaps("aaaa", "zzzz", false, false));
        assertFalse(f.overlaps("aaaa", "zzzz", false, true));

        // Null Testing
        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "d", null, "c");
        assertTrue(f.containsNull());

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Inverted,
                "A", "d", null, "c");
        assertFalse(f.containsNull());

        f = new MatchFilter(
                MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular,
                "A", "a", "b");
        assertFalse(f.containsNull());
    }

    public void testMatchFilterInteger() {
        MatchFilter f;

        // Integer filter, no match
        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 2);
        assertFalse(f.overlaps(3, 5, false, false));
        assertFalse(f.overlaps(3, 5, true, true));

        // Integer filter, match lower
        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 3);
        assertFalse(f.overlaps(3, 5, false, false));
        assertTrue(f.overlaps(3, 5, true, false));

        // Integer filter, match upper
        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 5);
        assertFalse(f.overlaps(3, 5, false, false));
        assertTrue(f.overlaps(3, 5, false, true));

        // Integer filter, match middle
        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 4);
        assertTrue(f.overlaps(3, 5, false, false));
        assertTrue(f.overlaps(3, 5, true, true));

        // Inverted versions of the above tests

        f = new MatchFilter(MatchFilter.MatchType.Inverted,
                "A", 1, 2);
        assertTrue(f.overlaps(3, 5, false, false));
        assertTrue(f.overlaps(3, 5, true, true));

        f = new MatchFilter(MatchFilter.MatchType.Inverted,
                "A", 1, 3);
        assertTrue(f.overlaps(3, 5, false, false));
        assertFalse(f.overlaps(3, 5, true, false));

        f = new MatchFilter(MatchFilter.MatchType.Inverted,
                "A", 1, 5);
        assertTrue(f.overlaps(3, 5, false, false));
        assertFalse(f.overlaps(3, 5, false, true));

        f = new MatchFilter(MatchFilter.MatchType.Inverted,
                "A", 1, 4);
        assertFalse(f.overlaps(3, 5, false, false));
        assertFalse(f.overlaps(3, 5, true, true));

        // Null testing
        f = new MatchFilter(MatchFilter.MatchType.Regular,
            "A", 1, 4, NULL_INT);
        assertTrue(f.containsNull());

        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 4, null);
        assertTrue(f.containsNull());

        f = new MatchFilter(MatchFilter.MatchType.Regular,
                "A", 1, 2);
        assertFalse(f.containsNull());
    }
}

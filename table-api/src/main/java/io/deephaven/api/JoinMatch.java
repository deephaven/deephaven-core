/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A join match represents one column from a {@link #left() left} table and one column from a {@link #right() right}
 * table. The exact semantics of the match depend on context. For example, a natural-join has equal-to matches;
 * where-not-in has not-equal-to matches; and as-of-join's last match has less-than or less-than-or-equal-to matches.
 *
 * @see TableOperations#join(Object, Collection, Collection, int)
 * @see TableOperations#naturalJoin(Object, Collection, Collection)
 * @see TableOperations#exactJoin(Object, Collection, Collection)
 * @see TableOperations#aj(Object, Collection, Collection, AsOfJoinRule)
 * @see TableOperations#raj(Object, Collection, Collection, ReverseAsOfJoinRule)
 * @see TableOperations#whereIn(Object, Collection)
 * @see TableOperations#whereNotIn(Object, Collection)
 */
public interface JoinMatch extends Serializable {

    static Collection<ColumnName> lefts(Collection<? extends JoinMatch> matches) {
        return matches.stream().map(JoinMatch::left).collect(Collectors.toList());
    }

    static Collection<ColumnName> rights(Collection<? extends JoinMatch> matches) {
        return matches.stream().map(JoinMatch::right).collect(Collectors.toList());
    }

    static JoinMatch of(ColumnName left, ColumnName right) {
        if (left.equals(right)) {
            return left;
        }
        return JoinMatchImpl.of(left, right);
    }

    static JoinMatch parse(String x) {
        final int ix = x.indexOf('=');
        if (ix < 0) {
            return ColumnName.parse(x);
        }
        if (ix + 1 == x.length()) {
            throw new IllegalArgumentException(String.format(
                    "Unable to parse match '%s', expected form '<left>==<right>' or `<left>=<right>`",
                    x));
        }
        final int ix2 = x.charAt(ix + 1) == '=' ? ix + 1 : ix;
        if (ix2 + 1 == x.length()) {
            throw new IllegalArgumentException(String.format(
                    "Unable to parse match '%s', expected form '<left>==<right>' or `<left>=<right>`",
                    x));
        }
        ColumnName left = ColumnName.parse(x.substring(0, ix));
        ColumnName right = ColumnName.parse(x.substring(ix2 + 1));
        return of(left, right);
    }

    static List<JoinMatch> from(String... values) {
        return JoinMatch.from(Arrays.asList(values));
    }

    static List<JoinMatch> from(Collection<String> values) {
        return values.stream().map(JoinMatch::parse).collect(Collectors.toList());
    }

    /**
     * The column from the left table.
     *
     * @return the left column name
     */
    ColumnName left();

    /**
     * The column name from the right table.
     * 
     * @return the right column name
     */
    ColumnName right();
}

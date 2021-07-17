/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.db.tables.utils.NameValidator;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Holds a pair of column names.
 */
public class MatchPair implements Serializable {
    private static final long serialVersionUID = 20180822L;

    public static final MatchPair [] ZERO_LENGTH_MATCH_PAIR_ARRAY = new MatchPair[0];

    public static MatchPair of(Pair pair) {
        return new MatchPair(pair.output().name(), pair.input().name());
    }

    public static MatchPair of(JoinMatch match) {
        return new MatchPair(match.left().name(), match.right().name());
    }

    public static MatchPair of(JoinAddition addition) {
        return new MatchPair(addition.newColumn().name(), addition.existingColumn().name());
    }

    public static MatchPair[] fromMatches(Collection<? extends JoinMatch> matches) {
        return matches.stream().map(MatchPair::of).toArray(MatchPair[]::new);
    }

    public static MatchPair[] fromAddition(Collection<? extends JoinAddition> matches) {
        return matches.stream().map(MatchPair::of).toArray(MatchPair[]::new);
    }

    public static MatchPair[] fromPairs(Collection<? extends Pair> pairs) {
        return pairs.stream().map(MatchPair::of).toArray(MatchPair[]::new);
    }

    public static Stream<ColumnName> outputs(Collection<MatchPair> pairs) {
        return pairs.stream().map(MatchPair::left).map(ColumnName::of);
    }

    public final String leftColumn;
    public final String rightColumn;

    @Override
    public String toString() {
        return leftColumn + "=" + rightColumn;
    }

    /**
     * Inputs must be valid column names
     *
     * @param leftColumn LHS of the pair
     * @param rightColumn RHS of the pair
     */
    public MatchPair(String leftColumn,String rightColumn){
        this.leftColumn = NameValidator.validateColumnName(leftColumn);
        this.rightColumn = NameValidator.validateColumnName(rightColumn);
    }

    public String left() {
        return leftColumn;
    }

    public String right() {
        return rightColumn;
    }

    public static String[] getLeftColumns(MatchPair... matchPairs) {
        return Arrays.stream(matchPairs).map(MatchPair::left).toArray(String[]::new);
    }

    public static String[] getRightColumns(MatchPair... matchPairs) {
        return Arrays.stream(matchPairs).map(MatchPair::right).toArray(String[]::new);
    }

    public static final LogOutput.ObjFormatter<MatchPair[]> MATCH_PAIR_ARRAY_FORMATTER = (logOutput, matchPairs) -> {
        if ( matchPairs == null ) {
            logOutput.append("null");
        }
        else {
            boolean first = true;
            logOutput.append('[');
            for (MatchPair mp : matchPairs) {
                if (!first) {
                    logOutput.append(", ");
                }
                if (mp.left().equals(mp.right())) {
                    logOutput.append(mp.left());
                } else {
                    logOutput.append(mp.left()).append('=').append(mp.right());
                }
                first = false;
            }
            logOutput.append(']');
        }
    };

    public static final LogOutput.ObjFormatter<MatchPair> MATCH_PAIR_FORMATTER = (logOutput, mp) -> {
        if ( mp == null ) {
            logOutput.append("null");
        }
        else {
            if (mp.left().equals(mp.right())) {
                logOutput.append(mp.left());
            } else {
                logOutput.append(mp.left()).append('=').append(mp.right());
            }
        }
    };

    public static String matchString(final MatchPair[] matchPairArray) {
        return new LogOutputStringImpl().append(MATCH_PAIR_ARRAY_FORMATTER, matchPairArray).toString();
    }

    public static String matchString(final MatchPair matchPair) {
        return new LogOutputStringImpl().append(MATCH_PAIR_FORMATTER, matchPair).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MatchPair matchPair = (MatchPair) o;
        return Objects.equals(leftColumn, matchPair.leftColumn) &&
                Objects.equals(rightColumn, matchPair.rightColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftColumn, rightColumn);
    }
}

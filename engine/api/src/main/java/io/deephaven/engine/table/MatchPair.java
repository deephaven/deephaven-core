/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.api.util.NameValidator;

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
public class MatchPair implements Pair, JoinMatch, JoinAddition, Serializable {
    private static final long serialVersionUID = 20180822L;

    public static final MatchPair[] ZERO_LENGTH_MATCH_PAIR_ARRAY = new MatchPair[0];

    public static MatchPair of(Pair pair) {
        return pair instanceof MatchPair
                ? (MatchPair) pair
                : new MatchPair(pair.output().name(), pair.input().name());
    }

    public static MatchPair of(JoinMatch match) {
        return match instanceof MatchPair
                ? (MatchPair) match
                : new MatchPair(match.left().name(), match.right().name());
    }

    public static MatchPair of(JoinAddition addition) {
        return addition instanceof MatchPair
                ? (MatchPair) addition
                : new MatchPair(addition.newColumn().name(), addition.existingColumn().name());
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
        return pairs.stream().map(MatchPair::output);
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
    public MatchPair(String leftColumn, String rightColumn) {
        this.leftColumn = NameValidator.validateColumnName(leftColumn);
        this.rightColumn = NameValidator.validateColumnName(rightColumn);
    }

    public String leftColumn() {
        return leftColumn;
    }

    public String rightColumn() {
        return rightColumn;
    }

    public static String[] getLeftColumns(MatchPair... matchPairs) {
        return Arrays.stream(matchPairs).map(MatchPair::leftColumn).toArray(String[]::new);
    }

    public static String[] getRightColumns(MatchPair... matchPairs) {
        return Arrays.stream(matchPairs).map(MatchPair::rightColumn).toArray(String[]::new);
    }

    public static final LogOutput.ObjFormatter<MatchPair[]> MATCH_PAIR_ARRAY_FORMATTER = (logOutput, matchPairs) -> {
        if (matchPairs == null) {
            logOutput.append("null");
        } else {
            boolean first = true;
            logOutput.append('[');
            for (MatchPair mp : matchPairs) {
                if (!first) {
                    logOutput.append(", ");
                }
                if (mp.leftColumn().equals(mp.rightColumn())) {
                    logOutput.append(mp.leftColumn());
                } else {
                    logOutput.append(mp.leftColumn()).append('=').append(mp.rightColumn());
                }
                first = false;
            }
            logOutput.append(']');
        }
    };

    public static final LogOutput.ObjFormatter<MatchPair> MATCH_PAIR_FORMATTER = (logOutput, mp) -> {
        if (mp == null) {
            logOutput.append("null");
        } else {
            if (mp.leftColumn().equals(mp.rightColumn())) {
                logOutput.append(mp.leftColumn());
            } else {
                logOutput.append(mp.leftColumn()).append('=').append(mp.rightColumn());
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final MatchPair matchPair = (MatchPair) o;
        return Objects.equals(leftColumn, matchPair.leftColumn) &&
                Objects.equals(rightColumn, matchPair.rightColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftColumn, rightColumn);
    }

    // region Pair impl

    @Override
    public ColumnName input() {
        return ColumnName.of(rightColumn);
    }

    @Override
    public ColumnName output() {
        return ColumnName.of(leftColumn);
    }

    // endregion Pair impl

    // region JoinMatch impl

    @Override
    public ColumnName left() {
        return ColumnName.of(leftColumn);
    }

    @Override
    public ColumnName right() {
        return ColumnName.of(rightColumn);
    }

    // endregion JoinMatch impl

    // region JoinAddition impl

    @Override
    public ColumnName newColumn() {
        return ColumnName.of(leftColumn);
    }

    @Override
    public ColumnName existingColumn() {
        return ColumnName.of(rightColumn);
    }

    // endregion JoinAddition impl
}

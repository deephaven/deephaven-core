/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.db.tables.utils.DBNameValidator;

import io.deephaven.qst.table.ColumnAssignment;
import io.deephaven.qst.table.ColumnMatch;
import io.deephaven.qst.table.ColumnName;
import io.deephaven.qst.table.JoinAddition;
import io.deephaven.qst.table.JoinMatch;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Holds a pair of column names.
 */
public class MatchPair implements Serializable {
    private static final long serialVersionUID = 20180822L;

    public static final MatchPair [] ZERO_LENGTH_MATCH_PAIR_ARRAY = new MatchPair[0];

    public static MatchPair of(JoinMatch joinMatch) {
        return joinMatch.walk(new JoinMatchVisitor()).getOut();
    }

    public static MatchPair of(JoinAddition joinAddition) {
        return joinAddition.walk(new JoinAdditionVisitor()).getOut();
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
        this.leftColumn = DBNameValidator.validateColumnName(leftColumn);
        this.rightColumn = DBNameValidator.validateColumnName(rightColumn);
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

    private static class JoinMatchVisitor implements JoinMatch.Visitor {
        private MatchPair out;

        public MatchPair getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = new MatchPair(columnName.name(), columnName.name());
        }

        @Override
        public void visit(ColumnMatch columnMatch) {
            out = new MatchPair(columnMatch.left().name(), columnMatch.right().name());
        }
    }

    private static class JoinAdditionVisitor implements JoinAddition.Visitor {
        private MatchPair out;

        public MatchPair getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName columnName) {
            out = new MatchPair(columnName.name(), columnName.name());
        }

        @Override
        public void visit(ColumnAssignment columnAssignment) {
            out = new MatchPair(
                columnAssignment.newColumn().name(),
                columnAssignment.existingColumn().name());
        }
    }
}

package io.deephaven.api;

import java.util.Objects;

/**
 * A set of static helpers to turn strongly-typed api arguments into their {@link String}
 * counterparts.
 */
public class Strings {

    public static String of(ColumnName columnName) {
        return columnName.name();
    }

    public static String of(RawString rawString) {
        return rawString.value();
    }

    public static String of(ColumnMatch match) {
        return String.format("%s==%s", of(match.left()), of(match.right()));
    }

    public static String of(ColumnFormula formula) {
        return String.format("%s=%s", of(formula.newColumn()), of(formula.expression()));
    }

    public static String of(ColumnAssignment assignment) {
        return String.format("%s=%s", of(assignment.newColumn()), of(assignment.existingColumn()));
    }

    public static String of(Selectable selectable) {
        return selectable.walk(new UniversalAdapter()).getOut();
    }

    public static String of(Expression expression) {
        return expression.walk(new UniversalAdapter()).getOut();
    }

    public static String of(Filter filter) {
        return filter.walk(new UniversalAdapter()).getOut();
    }

    public static String of(JoinAddition joinAddition) {
        return joinAddition.walk(new UniversalAdapter()).getOut();
    }

    public static String of(JoinMatch joinMatch) {
        return joinMatch.walk(new UniversalAdapter()).getOut();
    }

    /**
     * If we ever need to provide more specificity for a type, we can create a non-universal impl.
     */
    private static class UniversalAdapter implements Filter.Visitor, Expression.Visitor,
        Selectable.Visitor, JoinAddition.Visitor, JoinMatch.Visitor {
        private String out;

        public String getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ColumnName name) {
            out = of(name);
        }

        @Override
        public void visit(RawString rawString) {
            out = of(rawString);
        }

        @Override
        public void visit(ColumnFormula columnFormula) {
            out = of(columnFormula);
        }

        @Override
        public void visit(ColumnAssignment columnAssignment) {
            out = of(columnAssignment);
        }

        @Override
        public void visit(ColumnMatch columnMatch) {
            out = of(columnMatch);
        }
    }
}

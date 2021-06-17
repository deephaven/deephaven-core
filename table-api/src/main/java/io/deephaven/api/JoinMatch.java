package io.deephaven.api;

public interface JoinMatch {

    static JoinMatch parse(String x) {
        final int eqIx = x.indexOf('=');
        if (eqIx >= 0) {
            // todo
            ColumnName left = ImmutableColumnName.of(x.substring(0, eqIx));
            ColumnName right = ImmutableColumnName.of(x.substring(eqIx + 1));
            return ImmutableColumnMatch.of(left, right);
        } else {
            return ImmutableColumnName.of(x);
        }
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnMatch columnMatch);

        // todo: can you match against an expression?
    }
}

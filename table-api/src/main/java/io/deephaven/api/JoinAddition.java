package io.deephaven.api;

public interface JoinAddition {

    static JoinAddition parse(String x) {
        final int eqIx = x.indexOf('=');
        if (eqIx >= 0) {
            // todo
            ColumnName newCol = ImmutableColumnName.of(x.substring(0, eqIx));
            ColumnName existingCol = ImmutableColumnName.of(x.substring(eqIx + 1));
            return ImmutableColumnAssignment.of(newCol, existingCol);
        } else {
            return ImmutableColumnName.of(x);
        }
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnAssignment columnAssignment);

        // todo: can you match against an expression?
    }
}

package io.deephaven.api;

public interface JoinAddition {

    static JoinAddition parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ColumnName.of(x);
        }
        return ColumnAssignment.parse(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnAssignment columnAssignment);
    }
}

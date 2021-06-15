package io.deephaven.qst.table;

public interface Expression {

    static Expression parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ImmutableColumnName.of(x);
        }
        return ImmutableRawString.of(x);
    }

    <V extends Visitor> V walk(V visitor);

    // todo: expand expression as fuller AST
    interface Visitor {
        void visit(ColumnName name);

        void visit(RawString rawString);
    }
}

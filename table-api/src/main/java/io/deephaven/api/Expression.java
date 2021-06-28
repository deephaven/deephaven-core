package io.deephaven.api;

public interface Expression {

    static Expression parse(String x) {
        // In this context, we can't know if the RHS is a column or not.
        // if (ColumnName.isValidParsedColumnName(x)) {
        // return ColumnName.parse(x);
        // }
        return RawString.of(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(RawString rawString);
    }
}

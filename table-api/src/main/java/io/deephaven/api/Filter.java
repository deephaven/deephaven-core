package io.deephaven.api;

public interface Filter {
    static Filter parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ColumnName.of(x);
        }
        return RawString.of(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(RawString rawString);
    }
}

package io.deephaven.qst.table;

public interface Filter {
    static Filter parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ImmutableColumnName.of(x);
        }
        return ImmutableRawString.of(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(RawString rawString);
    }
}

package io.deephaven.api;

public interface Filter {
    static Filter parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
        }
        // note: we are unable to parse the RHS strongly typed, so we can't infer ColumnMatch here.
        return RawString.of(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(ColumnMatch match);

        void visit(RawString rawString);
    }
}

package io.deephaven.qst.table;

public interface Selectable {

    static Selectable parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ImmutableColumnName.of(x); // todo
        }
        return ColumnFormula.parse(x);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnFormula columnFormula);
    }
}

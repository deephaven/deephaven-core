package io.deephaven.qst.table;

public interface SourceTable extends Table {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(EmptyTable emptyTable);
        void visit(NewTable newTable);
    }
}

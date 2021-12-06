package io.deephaven.qst.table;

/**
 * A table schema is used to refer to a schema. A schema may be referenced directly via {@link TableHeader}; or
 * indirectly via {@link TableSpec}, which means the schema is the same as the other {@link TableSpec} schema.
 */
public interface TableSchema {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(TableSpec spec);

        void visit(TableHeader header);
    }
}

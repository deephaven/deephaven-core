package io.deephaven.qst.table;

/**
 * An input table creation specification.
 *
 * @see InMemoryAppendOnlyInputTable
 * @see InMemoryKeyBackedInputTable
 */
public interface InputTable extends TableSpec {

    /**
     * The schema for the input table.
     *
     * @return the schema
     */
    TableSchema schema();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly);

        void visit(InMemoryKeyBackedInputTable inMemoryKeyBacked);
    }
}

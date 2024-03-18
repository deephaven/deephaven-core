//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {

        R visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly);

        R visit(InMemoryKeyBackedInputTable inMemoryKeyBacked);

        R visit(BlinkInputTable blinkInputTable);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

/**
 * A table schema is used to refer to a schema. A schema may be referenced directly via {@link TableHeader}; or
 * indirectly via {@link TableSpec}, which means the schema is the same as the other {@link TableSpec} schema.
 */
public interface TableSchema {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(TableSpec spec);

        T visit(TableHeader header);
    }
}

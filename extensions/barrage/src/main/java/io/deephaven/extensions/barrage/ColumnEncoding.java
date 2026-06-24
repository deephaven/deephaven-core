//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

/**
 * Arrow encoding to apply to a column when generating a Barrage schema. Auto-detected per column by
 * {@link io.deephaven.extensions.barrage.util.BarrageUtil#schemaFromTable} when structural guarantees make a particular
 * encoding clearly beneficial, or specified explicitly via
 * {@link io.deephaven.engine.table.Table#BARRAGE_SCHEMA_ATTRIBUTE}.
 */
public enum ColumnEncoding {
    // Int32 is default run-end encoding if not specified
    RUN_END_ENCODED_INT16, RUN_END_ENCODED_INT32, RUN_END_ENCODED_INT64, DICTIONARY_ENCODED;

    /** Returns {@code true} for any run-end encoding variant. */
    public boolean isRunEndEncoded() {
        return this == RUN_END_ENCODED_INT16 || this == RUN_END_ENCODED_INT32 || this == RUN_END_ENCODED_INT64;
    }
}
